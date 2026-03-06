#!/bin/bash
# run_test.sh — รัน tests ทั้งหมดผ่าน Docker
#
# Usage:
#   ./run_test.sh                  ← รันทุก test
#   ./run_test.sh tests/test_versioning.py  ← รัน file เดียว
#   ./run_test.sh tests/test_manage.py -v   ← พร้อม flags

TARGET=${1:-"tests/"}
EXTRA_ARGS="${@:2}"

docker exec -it spark-master bash -c "
  python3 -m pip install pytest pytest-html -q &&
  cd /jobs &&
  PYTHONPATH=/jobs:/spark/python:/spark/python/lib/py4j-0.10.7-src.zip \
  python3 -m pytest ${TARGET} ${EXTRA_ARGS} -v \
    --junitxml=reports/junit.xml \
    --html=reports/report.html --self-contained-html
"
docker cp spark-master:/jobs/reports/. ./reports/