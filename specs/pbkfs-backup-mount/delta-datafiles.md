# Delta storage for PostgreSQL datafiles in pbkfs

**Context**: `pbkfs` монтирует цепочку pg_probackup (FULL + INC) как PostgreSQL‑совместимый `PGDATA`, используя copy‑on‑write в `pbk_diff`.
Сейчас при любом изменении блока (включая hint bits, VACUUM и пр.) мы копируем целую 8‑KiB страницу в `pbk_diff`.
Если над смонтированным бэкапом прогнать полный проход (например, `pg_dump` или плотный seq scan), Postgres проставляет hint bits и может переписать значительную часть страниц → `pbk_diff` по объёму приближается к полной базе.

Цель этого документа — зафиксировать формат дельта‑хранилища: хранить в `pbk_diff` не полные страницы, а компактные дельты к базовому слою, чтобы объём diff был примерно пропорционален реальному объёму изменений в байтах, а не количеству затронутых страниц.

Реализация дельта‑хранилища планируется в **Phase 12** после того, как в Phase 10–11 будут внедрены worker‑pool для FUSE и handle‑based I/O с политикой `materialize_on_read = false`.

---

## Требования

1. **Не раздувать `pbk_diff` до размера полной базы** при read‑heavy workload'ах:
   - типичный сценарий: много чтений, немного пользовательских записей,
   - системные записи (hint bits, VACUUM/autovacuum) допустимы, но не должны автоматически означать «копируем весь кластер».
2. **Совместимость с текущей моделью Overlay/pg_block_index**:
   - не ломать уже реализованную логику чтения datafile'ов через pg_probackup page‑стримы,
   - дельта‑слой должен быть «поверх» существующего `pg_block_index`, а не вместо него.
3. **Минимальные допущения о формате страниц PostgreSQL**:
   - дельта считается побайтно (base_page vs modified_page),
   - не пытаемся разбирать layout heap‑страниц и отдельно идентифицировать hint bits,
   - решение должно быть устойчивым к изменениям формата страниц между версиями PostgreSQL (14–17).
4. **Сохранить высокую производительность чтения**:
   - sequential scan по страницам без дельт должен идти почти так же быстро, как чтение из `pbk_store`,
   - распаковка и применение дельт должно быть дешёвым на CPU.
5. **In-place update**:
   - каждый блок имеет ровно одну актуальную дельту (или не имеет дельты),
   - нет накопления «мёртвых» записей при повторных записях в один блок.

---

## Архитектура хранения

### Файлы per datafile

Для каждого PostgreSQL datafile создаётся пара файлов в `pbk_diff`:

```
pbk_diff/
└── base/
    └── 16389/
        ├── 16402.patch     # слоты для PATCH-дельт (sparse file)
        ├── 16402.full      # полные страницы для FULL-дельт (sparse file)
        ├── 16403.patch
        ├── 16403.full
        └── ...
```

- **`.patch`** — sparse file с фиксированными слотами 512 байт для маленьких дельт
- **`.full`** — sparse file для полных 8KB страниц (когда дельта слишком большая)

### Почему два файла?

| Тип дельты | Размер | Хранение |
|------------|--------|----------|
| PATCH (hint bits, мелкие изменения) | ≤ 504 байта | Слот 512 байт в .patch |
| FULL (серьёзные изменения, VACUUM) | 8192 байт | Страница 8KB в .full |

Разделение позволяет:
- Экономить место для маленьких дельт (слот 512B вместо 8KB)
- In-place update для PATCH (слот фиксированного размера)
- Punch hole при переходе FULL → PATCH (освобождение места)

---

## Формат файла .patch

### Заголовок (512 байт, выровнен)

```rust
#[repr(C)]
struct PatchFileHeader {          // 512 bytes total
    magic: [u8; 8],               // "PBKPATCH"
    version: u16,                 // 2 (v2 byte-stream format)
    flags: u16,                   // reserved
    page_size: u32,               // 8192
    slot_size: u32,               // 512
    reserved: [u8; 492],          // padding to 512 bytes
}
```

### Слоты (512 байт каждый)

Слоты адресуются напрямую по номеру блока:

```
offset(block_N) = 512 + N * 512 = (N + 1) * 512
```

```rust
#[repr(C)]
struct Slot {                     // 512 bytes total
    kind: u8,                     // 0=EMPTY, 1=PATCH, 2=FULL_REF
    flags: u8,                    // bit 0 = v2 byte-stream payload (must be 1 for PATCH)
    payload_len: u16,             // для PATCH: длина данных (0..504)
    reserved: [u8; 4],            // выравнивание
    payload: [u8; 504],           // для PATCH: данные дельты
}
```

### Формат PATCH payload (v2 byte-stream)

Каждый изменённый байт кодируется как относительное смещение от предыдущего
изменённого байта и новое значение. Это даёт ≈2 байта на изменённый байт для
типичных hint‑bit страниц.

Состояние декодера: `pos = -1` в начале. Для каждой операции читается `delta`
и `value`:

- `delta` кодируется либо одним байтом (`0..254`), либо тремя байтами с префиксом
  `0xFF` и последующим `u16` (LE) для значений `255..65535`.
- Затем `pos = pos + 1 + delta`; проверяем `0 <= pos < 8192`.
- Записываем `page[pos] = value`.

Payload — это последовательность таких пар `(delta_code, value)` до конца
`payload_len`. Любые обрывы на середине delta/value или выход за границы
страницы считаются повреждёнными дельтами.

`payload_len = 0` при `kind = PATCH` по-прежнему невалидно.

### Версионирование и совместимость

- Формат v1 (сегменты offset/len/data) **официально снят с поддержки** и считается устаревшим. Документ `delta-datafiles.md` теперь описывает только v2; прежнее описание v1 рассматривается как историческое.
- Все `.patch` файлы должны иметь `version = 2` и `flags & 0x01 != 0` для PATCH-слотов.
- При обновлении требуется очистить/пересоздать существующие diff-директории, так как смешанные v1/v2 не поддерживаются.
- Дизайн и мотивация нового формата подробно изложены в `specs/pbkfs-backup-mount/delta-new-patch-format.md`, который является основным источником по payload‑формату.

### Sparse поведение

Файл `.patch` является sparse:
- При создании: `ftruncate()` до нужного размера без записи
- EMPTY слоты — дыры в файле (не занимают место на диске)
- При чтении из дыры возвращаются нули → `kind=0` (EMPTY)

### Запись отдельного слота и выравнивание 4KB

- По умолчанию используем обычный buffered `pwrite` на 512 байт по offset слота — ОС сама округлит до страницы в page cache, это корректно для sparse-файла.
- Если выбран режим O_DIRECT/4KB-выравнивание, выполняем read-modify-write ближайшего 4KB блока: читаем 4KB, обновляем нужный слот, остальные слоты в блоке заполняем нулями (интерпретируются как `EMPTY`), записываем обратно. Bitmap меняем только для целевого блока; нулевые слоты не требуют правок в bitmap (они уже `00`).

---

## Формат файла .full

### Заголовок (4096 байт, выровнен по блоку FS)

```rust
#[repr(C)]
struct FullFileHeader {           // 4096 bytes total
    magic: [u8; 8],               // "PBKFULL\0"
    version: u16,                 // 1
    flags: u16,                   // reserved
    page_size: u32,               // 8192
    reserved: [u8; 4080],         // padding to 4096 bytes
}
```

### Страницы (8192 байт каждая, детерминистичный offset)

```
offset для блока N = 4096 + N * 8192
```

Offset **детерминистичен** — вычисляется по номеру блока. Файл `.full` является **sparse**:
- При записи FULL для блока N → `pwrite` по offset `4096 + N * 8192`
- Блоки без FULL → дыры в sparse file (не занимают место)
- При переходе FULL → PATCH → `punch_hole` по тому же offset

**Следствие:**
- Логический размер `.full` = `4096 + (max_block_with_full + 1) * 8192`
- Физический размер (du) пропорционален количеству реальных FULL страниц
- Для таблицы 10GB с FULL только на последнем блоке: логический ~10GB, физический ~12KB

### Sparse поведение и punch hole

При переходе FULL → PATCH:
1. Записываем PATCH в слот `.patch`
2. Обновляем запись в bitmap
3. Вычисляем offset: `4096 + block_no * 8192`
4. Вызываем `fallocate(FALLOC_FL_PUNCH_HOLE)` по этому offset в `.full`

```rust
fn punch_hole(fd: RawFd, offset: u64, len: u64) -> io::Result<()> {
    // Поддерживается: ext4, XFS, Btrfs, ZFS
    // Fallback при ошибке: оставляем мёртвую запись, warn в лог
    unsafe {
        libc::fallocate(
            fd,
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            offset as i64,
            len as i64
        )
    }
}
```

---

## Per-file Bitmap индекс

### Проблема

Для каждого блока нужно быстро определить: есть ли дельта, и какого она типа (PATCH или FULL)?

Наивный подход — читать слот с диска для каждого блока — дорогой при sequential scan.

### Решение: bitmap per datafile

Для каждой пары `.patch`/`.full` хранится битовая карта в памяти:

```rust
/// 2 бита на блок: тип дельты
/// 00 = EMPTY (нет дельты, читать base_page)
/// 10 = PATCH (читать слот из .patch)
/// 01 = FULL  (читать страницу из .full)
/// 11 = reserved (ошибка при обнаружении)
struct BlockBitmap {
    /// RwLock per-bitmap: читатели параллельны, писатели эксклюзивны
    bits: RwLock<Vec<u8>>,  // packed: 4 блока на байт
}

impl BlockBitmap {
    fn get(&self, block_no: u32) -> u8 {
        let bits = self.bits.read();
        let byte_idx = block_no as usize / 4;
        let bit_offset = (block_no % 4) * 2;
        (bits.get(byte_idx).copied().unwrap_or(0) >> bit_offset) & 0b11
    }

    fn set(&self, block_no: u32, value: u8) {
        let mut bits = self.bits.write();
        let byte_idx = block_no as usize / 4;
        let bit_offset = (block_no % 4) * 2;
        if byte_idx >= bits.len() {
            bits.resize(byte_idx + 1, 0);
        }
        bits[byte_idx] &= !(0b11 << bit_offset);
        bits[byte_idx] |= (value & 0b11) << bit_offset;
    }
}
```

**Почему RwLock per-bitmap:**
- Читатели параллельны (важно для seq scan)
- PostgreSQL buffer manager сериализует writes → низкая конкуренция
- Простая реализация, достаточная для нашего workload

### Индекс bitmap'ов

```rust
struct DeltaIndex {
    /// HashMap: hash(path) → bitmap
    /// Загружается лениво при первом обращении к datafile
    ///
    /// Примечание: используем u64 hash как ключ для экономии памяти.
    /// Коллизия крайне маловероятна (2^64 пространство) и не критична —
    /// в худшем случае два файла получат одну bitmap, что приведёт
    /// к лишним disk I/O, но не к corruption.
    bitmaps: RwLock<HashMap<u64, BlockBitmap>>,
}

fn get_bitmap(path: &Path) -> Option<&BlockBitmap> {
    let path_hash = hash(path);

    // Fast path: уже загружена (read lock)
    if let Some(bitmap) = bitmaps.read().get(&path_hash) {
        return Some(bitmap);
    }

    // Slow path: write lock + double-checked locking
    let mut bitmaps = bitmaps.write();

    // Double-check: другой thread мог загрузить пока мы ждали write lock
    if let Some(bitmap) = bitmaps.get(&path_hash) {
        return Some(bitmap);
    }

    // Lazy load: сканируем .patch файл и строим bitmap
    if patch_file_exists(path) {
        let bitmap = load_bitmap_from_patch(path)?;
        bitmaps.insert(path_hash, bitmap);
        return bitmaps.get(&path_hash);
    }

    None  // Файл не менялся
}
```

### Размер bitmap

```
1 сегмент (1GB) = 131,072 блоков
131,072 блоков × 2 бита = 262,144 бит = 32 KB на сегмент
```

### Оценка памяти для разных размеров БД

| Размер БД | Файлов (прибл.) | Bitmap всего | HashMap overhead | Итого |
|-----------|-----------------|--------------|------------------|-------|
| 100 GB | 5,000 | 3.2 MB | 240 KB | ~3.5 MB |
| 1 TB | 50,000 | 32 MB | 2.4 MB | ~35 MB |
| 10 TB | 500,000 | 320 MB | 24 MB | ~350 MB |
| 100 TB | 5,000,000 | 3.2 GB | 240 MB | ~3.5 GB |

### Загрузка bitmap при старте

При первом обращении к datafile:
1. Открыть `.patch` файл
2. Прочитать и **валидировать header** (magic, version)
3. Определить количество слотов по размеру файла
4. Для каждого слота: читать первый байт (kind)
5. Построить bitmap

Доп. валидация для `.full`:
- Если `.full` существует, вычисляем `max_full_block = (size - 4096) / 8192 - 1` (для size > 4096).
- Для каждого блока `0..=max_full_block` должен существовать слот `FULL_REF` в `.patch`; если слот отсутствует или `kind != FULL_REF`, считаем формат повреждённым (CorruptedBitmap/CorruptedDelta) и отклоняем монтирование.
- Ситуация «есть .full, но нет .patch» трактуется как ошибка формата.

```rust
fn load_bitmap_from_patch(path: &Path) -> Result<BlockBitmap, Error> {
    let patch = File::open(patch_file_path(path))?;

    // 1. Валидация header
    let mut header = [0u8; 512];
    patch.read_at(&mut header, 0)?;
    if &header[0..8] != b"PBKPATCH" {
        return Err(Error::InvalidMagic("expected PBKPATCH"));
    }
    let version = u16::from_le_bytes([header[8], header[9]]);
    if version != 1 {
        return Err(Error::UnsupportedVersion(version));
    }

    // 2. Определяем количество слотов
    let file_size = patch.metadata()?.len();
    let num_slots = (file_size - 512) / 512;  // минус header

    let mut bitmap = BlockBitmap::new();

    // 3. Сканируем слоты (можно батчами для ускорения)
    for block_no in 0..num_slots as u32 {
        let slot_offset = 512 + block_no as u64 * 512;
        let mut kind_byte = [0u8; 1];
        patch.read_at(&mut kind_byte, slot_offset)?;

        match kind_byte[0] {
            0 => {},  // EMPTY — оставляем 00
            1 => bitmap.set(block_no, 0b10),  // PATCH
            2 => bitmap.set(block_no, 0b01),  // FULL_REF
            _ => return Err(Error::CorruptedSlot("invalid kind")),
        }
    }

    Ok(bitmap)
}
```

**Оптимизация (обязательно):** слоты читаем батчами (≥1MB) для ускорения загрузки при построении bitmap.

---

## Алгоритмы

### Чтение блока

```rust
fn read_block(path: &Path, block_no: u32) -> Page {
    // 1. Получаем bitmap (lazy load при первом обращении)
    let bitmap = match get_bitmap(path) {
        Some(bm) => bm,
        None => {
            // Нет .patch файла — файл не менялся
            return read_base_page(path, block_no);
        }
    };

    // 2. Проверяем тип блока по bitmap (O(1), без disk I/O)
    match bitmap.get(block_no) {
        0b00 => {
            // EMPTY — читаем base_page из pbk_store
            read_base_page(path, block_no)
        }
        0b10 => {
            // PATCH — читаем слот и применяем дельту
            let slot_offset = (block_no as u64 + 1) * 512;
            let slot: Slot = pread(&patch_file_path(path), slot_offset, 512);
            validate_patch_slot(&slot)?;

            let base_page = read_base_page(path, block_no);
            apply_patch(&base_page, &slot.payload[..slot.payload_len as usize])
        }
        0b01 => {
            // FULL — читаем страницу напрямую из .full
            let full_offset = 4096 + block_no as u64 * 8192;
            pread(&full_file_path(path), full_offset, 8192)
        }
        0b11 => {
            Err(Error::CorruptedBitmap("reserved bits 11 encountered"))
        }
        _ => unreachable!(),
    }
}

fn validate_patch_slot(slot: &Slot) -> Result<(), Error> {
    if slot.kind != PATCH {
        return Err(Error::CorruptedSlot("expected PATCH, got different kind"));
    }
    if slot.payload_len == 0 {
        return Err(Error::CorruptedSlot("PATCH with payload_len=0"));
    }
    if slot.payload_len > 504 {
        return Err(Error::CorruptedSlot("payload_len > 504"));
    }
    Ok(())
}
```

### Запись блока

PostgreSQL всегда пишет полные 8KB страницы через buffer manager.

```rust
fn write_block(path: &Path, block_no: u32, new_page: &[u8; 8192]) {
    // 1. Восстанавливаем base_page из pbk_store (игнорируем текущую дельту!)
    //    Для новых блоков (block_no >= размер файла в pbk_store) — возвращает нули
    let base_page = read_base_page(path, block_no);

    // 2. Вычисляем дельту base_page → new_page
    let delta = compute_delta(&base_page, new_page);

    // 3. Получаем bitmap и текущий тип блока
    let bitmap = get_or_create_bitmap(path);
    let old_type = bitmap.get(block_no);

    // 4. Особый случай: new_page == base_page (идентичная страница)
    if delta.segments.is_empty() {
        if old_type == 0b00 {
            // Был EMPTY, остаётся EMPTY — ничего не делаем
            return;
        }
        // Был PATCH или FULL — очищаем (страница вернулась к base)
        punch_hole_slot(path, block_no);
        if old_type == 0b01 {
            let full_offset = 4096 + block_no as u64 * 8192;
            punch_hole(&full_file_path(path), full_offset, 8192);
        }
        bitmap.set(block_no, 0b00);  // EMPTY
        return;
    }

    // 5. Записываем в зависимости от размера дельты
    if delta.serialized_len() <= 504 {
        // PATCH — помещается в слот
        write_patch_slot(path, block_no, &delta);

        // Если был FULL → punch hole по детерминистичному offset
        if old_type == 0b01 {
            let full_offset = 4096 + block_no as u64 * 8192;
            punch_hole(&full_file_path(path), full_offset, 8192);
        }

        bitmap.set(block_no, 0b10);  // PATCH
    } else {
        // FULL — дельта слишком большая
        let full_offset = 4096 + block_no as u64 * 8192;
        write_full_page(path, full_offset, new_page);
        write_full_ref_slot(path, block_no);

        // punch hole не нужен — пишем по тому же offset

        bitmap.set(block_no, 0b01);  // FULL
    }
}

fn write_full_ref_slot(path: &Path, block_no: u32) {
    // Записываем слот с kind=FULL_REF
    // payload_len и payload не используются для FULL_REF
    let slot = Slot {
        kind: FULL_REF,
        flags: 0,
        payload_len: 0,
        reserved: [0; 4],
        payload: [0; 504],
    };
    let slot_offset = (block_no as u64 + 1) * 512;
    pwrite(&patch_file_path(path), slot_offset, &slot);
}
```

### Вычисление дельты

```rust
fn compute_delta(base: &[u8; 8192], new: &[u8; 8192]) -> Delta {
    let mut segments = Vec::new();
    let mut i = 0;

    while i < 8192 {
        // Пропускаем одинаковые байты
        while i < 8192 && base[i] == new[i] {
            i += 1;
        }
        if i >= 8192 {
            break;
        }

        // Находим конец отличающегося сегмента
        let start = i;
        while i < 8192 && base[i] != new[i] {
            i += 1;
        }
        // Расширяем сегмент, если следующее отличие близко (оптимизация)
        while i < 8192 && i - start < 64 {
            if base[i] != new[i] {
                while i < 8192 && base[i] != new[i] {
                    i += 1;
                }
            } else {
                i += 1;
            }
        }

        segments.push(PatchSegment {
            offset: start as u16,
            len: (i - start) as u16,
            data: new[start..i].to_vec(),
        });
    }

    Delta { segments }
}

impl Delta {
    fn serialized_len(&self) -> usize {
        self.segments.iter()
            .map(|s| 4 + s.data.len())  // offset:u16 + len:u16 + data
            .sum()
    }
}
```

### Применение дельты

```rust
fn apply_patch(base: &[u8; 8192], patch_data: &[u8]) -> Result<[u8; 8192], Error> {
    let mut result = *base;
    let mut cursor = 0;

    while cursor < patch_data.len() {
        // Проверяем что есть достаточно данных для header
        if cursor + 4 > patch_data.len() {
            return Err(Error::CorruptedPatch("truncated segment header"));
        }

        let offset = u16::from_le_bytes([patch_data[cursor], patch_data[cursor + 1]]) as usize;
        let len = u16::from_le_bytes([patch_data[cursor + 2], patch_data[cursor + 3]]) as usize;
        cursor += 4;

        // Валидация bounds
        if offset + len > 8192 {
            return Err(Error::CorruptedPatch("segment exceeds page size"));
        }
        if cursor + len > patch_data.len() {
            return Err(Error::CorruptedPatch("truncated segment data"));
        }

        result[offset..offset + len].copy_from_slice(&patch_data[cursor..cursor + len]);
        cursor += len;
    }

    Ok(result)
}
```

---

## Новые блоки (расширение datafile)

При CREATE TABLE или INSERT (extend relation) PostgreSQL записывает блоки за пределами файла в pbk_store:

```rust
fn read_base_page(path: &Path, block_no: u32) -> [u8; 8192] {
    let file_size = pbk_store_file_size(path);
    let max_block = file_size / 8192;

    if block_no >= max_block {
        // Новый блок — возвращаем нули
        return [0u8; 8192];
    }

    // Существующий блок — читаем из pbk_store/chain
    read_from_backup_chain(path, block_no)
}
```

**Следствие:** `compute_delta(zeros, new_page)` почти всегда даёт FULL, т.к. новая страница ≠ нули.

**При чтении нового блока (после записи):**
- Слот FULL_REF → возвращаем данные из `.full` (нормальный случай)
- Слот PATCH → apply_patch(zeros, patch) (редко, если новая страница почти пустая)

**Edge case:** Слот EMPTY + block_no >= pbk_store размер → возвращаем нули. Это возможно только если PostgreSQL расширил файл через ftruncate(), но ещё не записал данные. PostgreSQL ожидает нули в этом случае.

---

## Расширение .patch файла

При записи в блок за пределами текущего размера файла:

```rust
fn ensure_patch_file_size(path: &Path, block_no: u32) {
    let required_size = 512 + (block_no as u64 + 1) * 512;
    let current_size = patch_file(path).metadata().len();

    if required_size > current_size {
        // ftruncate расширяет файл, новые слоты — дыры (sparse)
        patch_file(path).set_len(required_size);
    }
}
```

---

## Создание .patch и .full файлов

### Когда создаются файлы

- **`.patch`**: создаётся при **первой записи** в datafile (любой блок)
- **`.full`**: создаётся **лениво** при первом FULL (когда дельта > 504 байта)

```rust
fn write_block(path: &Path, block_no: u32, new_page: &[u8; 8192]) {
    let patch_path = patch_file_path(path);

    // Создаём директорию и .patch при первой записи
    if !patch_path.exists() {
        // Директория может не существовать (CREATE TABLE в новой БД)
        if let Some(parent) = patch_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        create_patch_file(&patch_path)?;
        // Создаём bitmap для нового файла
        create_bitmap(path);
    }

    // ... compute delta ...

    if delta.serialized_len() <= 504 {
        write_patch_slot(...);
    } else {
        let full_path = full_file_path(path);
        // Создаём .full лениво
        if !full_path.exists() {
            create_full_file(&full_path)?;
        }
        write_full_page(...);
    }
}
```

### Сегменты datafile

PostgreSQL разбивает большие таблицы на сегменты по 1GB:

```
base/16389/16402      # segment 0 (первые 1GB)
base/16389/16402.1    # segment 1
base/16389/16402.2    # segment 2
```

Каждый сегмент — независимый файл со своими `.patch`/`.full`:

```
pbk_diff/base/16389/
├── 16402.patch       # дельты для segment 0
├── 16402.full
├── 16402.1.patch     # дельты для segment 1
├── 16402.1.full
├── 16402.2.patch     # дельты для segment 2
└── 16402.2.full
```

Fork-файлы (`_vm`, `_fsm`, `_init`) аналогично:

```
pbk_diff/base/16389/
├── 16402_vm.patch
├── 16402_vm.full
├── 16402_fsm.patch
└── 16402_fsm.full
```

---

## Миграция существующих diff-директорий

При переходе на Phase 12 существующие `pbk_diff` с полными копиями datafiles **не поддерживаются**.

**Требуется:**
1. `pbkfs unmount`
2. `pbkfs cleanup --diff-dir <path>`
3. Новый `pbkfs mount` — создаст diff в новом формате

Автоматическая миграция не предусмотрена в v1.

---

## Scope: только datafiles

| Файлы | Стратегия |
|-------|-----------|
| `base/*/*`, `global/*` | Дельты (.patch + .full) |
| Всё остальное (WAL, pg_control, pg_xact, configs, etc) | Full copy-up |

---

## Интеграция с существующей архитектурой

### Взаимодействие с pg_block_index

- `pg_block_index` остаётся источником `base_page` для всех блоков
- Дельта-слой работает поверх: `final_page = apply_delta(base_page, delta)`
- При отсутствии дельты: `final_page = base_page`

### Взаимодействие с materialize_on_read

- `materialize_on_read = false` (default): чтение не создаёт дельты
- `materialize_on_read = true`: поведение определяется отдельно (вне scope Phase 12)

### Взаимодействие с worker pool (Phase 10)

Тяжёлые операции выносятся в worker pool:
- Восстановление base_page (декомпрессия из pg_probackup)
- Применение PATCH
- Запись слота/страницы

---

## Failure semantics

### Обрыв записи

Каждая операция записи выполняется одним pwrite:
- Слот 512 байт — один pwrite (атомарно на большинстве FS)
- Страница 8KB — один pwrite

**Ограничение:** POSIX гарантирует атомарность записи только до `PIPE_BUF` (обычно 4KB). Запись 8KB может быть разделена на два блока FS. На практике современные FS (ext4, XFS) обрабатывают 8KB pwrite атомарно при выровненном offset, но это не гарантировано спецификацией.

**Следствие:** При crash во время записи FULL страницы возможна частично записанная страница. PostgreSQL защищается от этого full page writes в WAL, поэтому при recovery он восстановит корректные данные.

**Порядок операций критичен для crash safety:**

FULL → PATCH:
1. `write_patch_slot()` — записываем новый PATCH
2. `punch_hole()` — освобождаем старую FULL

PATCH → FULL:
1. `append_full_page()` — записываем новую FULL страницу
2. `write_full_ref_slot()` — обновляем слот на FULL_REF

При crash между операциями данные остаются корректными (возможна мёртвая запись в .full).

### Некорректный заголовок

При открытии .patch/.full проверяем:
- magic соответствует ожидаемому
- version поддерживается

При несовпадении — ошибка монтирования с понятным сообщением.

---

## Режим --perf-unsafe

Для максимальной производительности (в ущерб durability):

```bash
pbkfs mount ... --perf-unsafe
```

**Поведение:**
1. При монтировании создаётся `.pbkfs-dirty` marker в diff-dir
2. При `fsync()` от PostgreSQL — возвращаем OK без реального fsync
3. Отслеживаем dirty файлы в памяти
4. При graceful unmount — fsync всех dirty файлов, удаляем marker

**При crash:**
- `.pbkfs-dirty` остаётся
- Следующее монтирование отклоняется с ошибкой
- `--force` позволяет монтировать (с предупреждением о возможной потере данных)

---

## Метрики

Phase 12 добавляет следующие метрики в `src/logging/mod.rs`:

| Метрика | Описание |
|---------|----------|
| `delta_patch_count` | Количество блоков с PATCH |
| `delta_full_count` | Количество блоков с FULL |
| `delta_patch_avg_size` | Средний размер PATCH в байтах |
| `delta_bitmaps_loaded` | Количество загруженных bitmap'ов |
| `delta_bitmaps_total_bytes` | Суммарный размер bitmap'ов в памяти |
| `delta_punch_holes` | Количество punch hole операций |
| `delta_punch_hole_failures` | Неудачные punch hole (fallback) |

---

## Concurrency

### Двухуровневая блокировка

Для предотвращения race conditions используется двухуровневая схема блокировки:

1. **File-level RwLock** — для операций, затрагивающих весь файл (truncate, rename, unlink)
2. **Block-level RwLock** — для операций с отдельными блоками (read, write)

**Порядок захвата локов**: file lock → block lock (предотвращает deadlock)

### Per-block блокировка для чтения и записи

Конкурентные операции с одним блоком требуют синхронизации:
- **Read + Read**: параллельно (shared file lock + shared block lock)
- **Read + Write**: эксклюзивно (shared file lock + exclusive block lock)
- **Write + Write**: эксклюзивно (shared file lock + exclusive block lock)
- **Truncate/Rename**: exclusive file lock (блокирует все read/write)

```rust
struct DeltaStorage {
    /// Per-file bitmap: hash(path) → BlockBitmap
    bitmaps: Arc<RwLock<HashMap<u64, BlockBitmap>>>,
    /// Per-file RwLock для truncate/rename vs read/write
    file_locks: Arc<RwLock<HashMap<u64, Arc<RwLock<()>>>>>,
    /// Per-block RwLock для защиты read/write path
    block_locks: Arc<RwLock<HashMap<(u64, u32), Arc<RwLock<()>>>>>,
}

fn get_file_lock(path: &Path) -> Arc<RwLock<()>> {
    let hash = hash(path);
    let mut locks = file_locks.write();
    locks.entry(hash)
        .or_insert_with(|| Arc::new(RwLock::new(())))
        .clone()
}

fn get_block_lock(path: &Path, block_no: u32) -> Arc<RwLock<()>> {
    let key = (hash(path), block_no);
    let mut locks = block_locks.write();
    locks.entry(key)
        .or_insert_with(|| Arc::new(RwLock::new(())))
        .clone()
}

fn read_block(path: &Path, block_no: u32) -> Page {
    // 1. File-level shared lock (позволяет другим read/write, блокирует truncate/rename)
    let file_lock = get_file_lock(path);
    let _file_guard = file_lock.read();

    // 2. Block-level shared lock (позволяет другим читателям этого блока)
    let block_lock = get_block_lock(path, block_no);
    let _block_guard = block_lock.read();

    // ... read logic (проверка bitmap, чтение слота/страницы, apply_patch) ...

    // Очистка при выходе (автоматически через Drop)
    cleanup_block_lock(path, block_no, &block_lock);
}

fn write_block(path: &Path, block_no: u32, new_page: &[u8; 8192]) {
    // 1. File-level shared lock (позволяет другим read/write, блокирует truncate/rename)
    let file_lock = get_file_lock(path);
    let _file_guard = file_lock.read();

    // 2. Block-level exclusive lock (блокирует других читателей/писателей этого блока)
    let block_lock = get_block_lock(path, block_no);
    let _block_guard = block_lock.write();

    // ... write logic (обновление bitmap + слот/страница) ...

    // Очистка при выходе
    cleanup_block_lock(path, block_no, &block_lock);
}

fn cleanup_block_lock(path: &Path, block_no: u32, lock: &Arc<RwLock<()>>) {
    // Удаляем запись если мы единственный holder
    if Arc::strong_count(lock) == 1 {
        let key = (hash(path), block_no);
        let mut locks = block_locks.write();
        // Повторная проверка под write lock
        if let Some(entry) = locks.get(&key) {
            if Arc::strong_count(entry) == 1 {
                locks.remove(&key);
            }
        }
    }
}
```

**Очистка block_locks HashMap:**
- При снятии лока: удаляем запись если `Arc::strong_count == 1` (никто больше не ждёт)
- При `unlink` datafile — удаляем все локи для этого файла
- HashMap не растёт неограниченно — записи удаляются после использования

**Bitmap thread safety:**
- Bitmap защищена через `RwLock<HashMap<...>>`
- Чтение bitmap: shared lock на HashMap, затем shared доступ к конкретной bitmap
- Запись в bitmap: block_lock уже взят exclusive, обновляем bitmap под block_lock

**Почему RwLock, а не Mutex:**
- PostgreSQL buffer manager сериализует записи в один блок, но мы не полагаемся на это
- RwLock позволяет параллельные чтения (важно для seq scan)
- Write path берёт exclusive lock → гарантия консистентности

**Гарантии:**
- Read никогда не увидит частично записанный слот
- Bitmap всегда консистентна с файлами на диске

---

## File lifecycle operations

Все lifecycle операции используют **exclusive file lock** для предотвращения race conditions с concurrent read/write.

### unlink

При удалении datafile:
1. Взять **exclusive file lock**
2. Удалить `.patch` и `.full` файлы
3. Удалить bitmap из `bitmaps` HashMap (O(1))
4. Удалить все block_locks для этого файла
5. Удалить file_lock

```rust
fn unlink_datafile(path: &Path) {
    let file_lock = get_file_lock(path);
    let _guard = file_lock.write();  // exclusive lock

    // Теперь безопасно удалять — все readers/writers заблокированы
    fs::remove_file(patch_file_path(path)).ok();
    fs::remove_file(full_file_path(path)).ok();
    bitmaps.write().remove(&hash(path));
    cleanup_all_block_locks(path);
    file_locks.write().remove(&hash(path));
}
```

### truncate

- `truncate(0)`: взять exclusive file lock, удалить `.patch` и `.full` целиком, удалить bitmap
- `truncate(N)`:
  1. Взять **exclusive file lock**
  2. Punch hole в `.patch` для слотов за пределами нового размера
  3. Punch hole в `.full` для страниц за пределами
  4. Обновить bitmap: установить 00 для блоков >= N/8192

```rust
fn truncate_datafile(path: &Path, new_size: u64) {
    let file_lock = get_file_lock(path);
    let _guard = file_lock.write();  // exclusive lock

    let new_blocks = (new_size / 8192) as u32;

    // Punch holes и обновить bitmap для блоков >= new_blocks
    if let Some(bitmap) = bitmaps.read().get(&hash(path)) {
        for block_no in new_blocks.. {
            if bitmap.get(block_no) != 0b00 {
                punch_hole_slot(path, block_no);
                if bitmap.get(block_no) == 0b01 {
                    punch_hole_full(path, block_no);
                }
                bitmap.set(block_no, 0b00);
            } else {
                break;  // Остальные блоки уже EMPTY
            }
        }
    }
}
```

### rename

При `rename(A, B)`:
1. Взять **exclusive file locks для A и B** (в лексикографическом порядке для deadlock prevention)
2. Удалить bitmap для B (до удаления файлов)
3. Если B существует — удалить `B.patch`, `B.full`, удалить block_locks для B
4. Удалить bitmap для A (до переименования)
5. Переименовать `A.patch` → `B.patch`, `A.full` → `B.full`
6. Обновить block_locks: переименовать ключи hash(A) → hash(B)
7. При следующем обращении к B — bitmap будет загружена заново

```rust
fn rename_datafile(src: &Path, dst: &Path) {
    // Deadlock prevention: всегда берём локи в одном порядке
    let (first, second) = if src < dst { (src, dst) } else { (dst, src) };
    let lock1 = get_file_lock(first);
    let lock2 = get_file_lock(second);
    let _guard1 = lock1.write();
    let _guard2 = lock2.write();

    // Теперь безопасно переименовывать
    bitmaps.write().remove(&hash(dst));
    fs::remove_file(patch_file_path(dst)).ok();
    fs::remove_file(full_file_path(dst)).ok();
    cleanup_all_block_locks(dst);

    bitmaps.write().remove(&hash(src));
    fs::rename(patch_file_path(src), patch_file_path(dst)).ok();
    fs::rename(full_file_path(src), full_file_path(dst)).ok();
    rename_block_locks(src, dst);
}
```

**Важно:** File-level exclusive lock гарантирует, что никакие read/write операции не выполняются во время truncate/rename.

---

## Требования к файловой системе

| Функция | Минимальная версия |
|---------|-------------------|
| Sparse files | Linux 2.6.x (все современные FS) |
| `FALLOC_FL_PUNCH_HOLE` | Linux 2.6.38+ (ext4), 2.6.38+ (XFS), 3.0+ (Btrfs) |

**Поддерживаемые FS**: ext4, XFS, Btrfs, ZFS

**Ограничения:**
- TMPFS: sparse files работают, punch hole **не поддерживается**
- NFS: поведение зависит от сервера и версии протокола
- При отсутствии punch hole — fallback с предупреждением в лог (мёртвые записи накапливаются)

---

## Ограничения и будущая работа

1. **Sparse file зависимость**: требуется FS с поддержкой sparse files (ext4, XFS, Btrfs, ZFS)
2. **Punch hole зависимость**: при отсутствии поддержки — fallback с мёртвыми записями
3. **Логический размер .patch**: для datafile 2GB с изменением последнего блока — логический размер 128MB (физический минимален)
4. **Логический размер .full**: для datafile 10GB с FULL на последнем блоке — логический размер ~10GB (физический ~12KB)
5. **Копирование diff**: `cp`, `rsync`, `tar` без `--sparse` флага материализуют дыры
6. **Память для bitmap**: масштабируется линейно с размером БД (~35MB на 1TB, ~3.5GB на 100TB)
