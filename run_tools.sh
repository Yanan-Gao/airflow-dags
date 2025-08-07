#!/bin/sh

echo "Format code (yapf) -------------------------------------------"
docker run --rm \
   -v .:/tmp/code_checking \
   --entrypoint "/bin/bash" \
   --workdir "/tmp/code_checking" \
   --pull always dev.docker.adsrvr.org/dataproc/ttd-airflow:prod-latest \
   -c "yapf --in-place --recursive --parallel --style=code_tool_config/.style.yapf ."

echo ""
echo "Lint code (flake8) -------------------------------------------"
docker run --rm \
   -v .:/tmp/code_checking \
   --entrypoint "/bin/bash" \
   --workdir "/tmp/code_checking" \
   --pull always dev.docker.adsrvr.org/dataproc/ttd-airflow:prod-latest \
   -c "flake8 --append-config=code_tool_config/.flake8 ."

echo ""
echo "Typecheck code (mypy) -------------------------------------------"
docker run --rm \
   -v .:/tmp/code_checking \
   --entrypoint "/bin/bash" \
   --workdir "/tmp/code_checking" \
   --pull always dev.docker.adsrvr.org/dataproc/ttd-airflow:prod-latest \
   -c "mypy --config-file=code_tool_config/.mypy.ini ."
