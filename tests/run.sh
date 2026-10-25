#!/bin/sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
test_dir=$(mktemp -d)
trap 'rm -rf "$test_dir"' EXIT HUP INT TERM
compiler=${CC:-clang}

for lab in lab0 lab1; do
    if [ "$lab" = lab0 ]; then
        name=pa1
    else
        name=pa6
    fi
    source_dir="$project_dir/$lab/$name"
    work_dir="$test_dir/$lab"
    mkdir "$work_dir"

    "$compiler" -std=c99 -Wall -pedantic -Werror -I"$source_dir" \
        "$project_dir/tests/ipc_test.c" "$source_dir/ipc.c" -o "$work_dir/ipc_test"
    for case in roundtrip fragmented interleaved truncated bad_magic oversized eof_direct eof_any sigpipe; do
        timeout 20 "$work_dir/ipc_test" "$case" >"$work_dir/test.log" 2>"$work_dir/test.err"
        test ! -s "$work_dir/test.err"
    done

    if [ "$lab" = lab0 ]; then
        "$compiler" -std=c99 -Wall -pedantic -Werror "$source_dir"/*.c -o "$work_dir/$name"
    else
        timeout 20 "$work_dir/ipc_test" backpressure >"$work_dir/test.log" 2>"$work_dir/test.err"
        test ! -s "$work_dir/test.err"
        "$compiler" -std=c99 -Wall -pedantic -Werror "$source_dir"/*.c \
            -L"$source_dir" -lruntime -o "$work_dir/$name"
        "$compiler" -std=c99 -Wall -pedantic -Werror -I"$source_dir" \
            "$project_dir/tests/ipc_test.c" "$source_dir/ipc.c" \
            -L"$source_dir" -lruntime -o "$work_dir/ipc_runtime_test"
        LD_LIBRARY_PATH="$source_dir" timeout 20 "$work_dir/ipc_runtime_test" roundtrip \
            >"$work_dir/test.log" 2>"$work_dir/test.err"
        test ! -s "$work_dir/test.err"
    fi

    for count in 0 1 3; do
        (cd "$work_dir" && LD_LIBRARY_PATH="$source_dir" \
            timeout 30 "./$name" -p "$count" >stdout.log 2>stderr.log)
        test ! -s "$work_dir/stderr.log"
        if [ "$lab" = lab1 ]; then
            (cd "$work_dir" && LD_LIBRARY_PATH="$source_dir" \
                timeout 30 "./$name" -p "$count" --mutexl >stdout.log 2>stderr.log)
            test ! -s "$work_dir/stderr.log"
        fi
    done
    echo "$lab: OK"
done
