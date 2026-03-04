#!/bin/bash
TARGET=${1:-"tests/"}   # default รัน tests/ ทั้งหมด ถ้าไม่ระบุ

docker exec -it spark-master bash -c "
  python3 -m pip install pytest pytest-html -q &&
  cd /jobs &&
  PYTHONPATH=/spark/python:/spark/python/lib/py4j-0.10.7-src.zip \
  python3 -m pytest ${TARGET} -v \
    --junitxml=reports/junit.xml \
    --html=reports/report.html --self-contained-html
"
docker cp spark-master:/jobs/reports/. ./reports/