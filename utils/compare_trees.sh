#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 DIR1 DIR2" >&2
  exit 1
fi

DIR1="$1"
DIR2="$2"

if [ ! -d "$DIR1" ] || [ ! -d "$DIR2" ]; then
  echo "Both arguments must be existing directories" >&2
  exit 1
fi

DIR1_ABS="$(readlink -f "$DIR1")"
DIR2_ABS="$(readlink -f "$DIR2")"

TMP1="$(mktemp)"
TMP2="$(mktemp)"
trap 'rm -f "$TMP1" "$TMP2"' EXIT

# Собираем список всех путей (файлы и каталоги) относительно корня.
( cd "$DIR1_ABS" && find . -print | sed 's|^\./||' | sort ) >"$TMP1"
( cd "$DIR2_ABS" && find . -print | sed 's|^\./||' | sort ) >"$TMP2"

echo "=== Structure differences ==="
# Только в DIR1
comm -23 "$TMP1" "$TMP2" | while read -r rel; do
  [ -z "$rel" ] && continue
  echo "only in $DIR1_ABS: $rel"
done
# Только в DIR2
comm -13 "$TMP1" "$TMP2" | while read -r rel; do
  [ -z "$rel" ] && continue
  echo "only in $DIR2_ABS: $rel"
done

echo
echo "=== Type/content differences ==="
# Общие пути
comm -12 "$TMP1" "$TMP2" | while read -r rel; do
  [ -z "$rel" ] && continue

  P1="$DIR1_ABS/$rel"
  P2="$DIR2_ABS/$rel"

  # echo "    comparing $P1 and $P2"

  # Пропускаем корень (пустой путь)
  if [ "$rel" = "." ]; then
    continue
  fi

  # Если оба каталоги — типов и содержимого различий нет.
  if [ -d "$P1" ] && [ -d "$P2" ]; then
    continue
  fi

  # Типы объектов
  TYPE1="$(stat -c %F "$P1")"
  TYPE2="$(stat -c %F "$P2")"

  if [ "$TYPE1" != "$TYPE2" ]; then
    echo "type differs: $rel ($TYPE1 vs $TYPE2)"
    continue
  fi

  # Для обычных файлов сравниваем содержимое побайтно.
  if [ -f "$P1" ] && [ -f "$P2" ]; then
    if ! cmp -s "$P1" "$P2"; then
      echo "content differs: $rel"
    fi
  fi
done

