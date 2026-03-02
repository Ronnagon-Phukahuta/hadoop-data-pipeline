## ขั้นตอนหลังแก้ auth.py

### 1. เพิ่ม dependency
```bash
# requirements.txt ของ dashboard
echo "streamlit-cookies-manager" >> dashboard/requirements.txt
```

### 2. เพิ่ม COOKIE_SECRET ใน .env
```env
COOKIE_SECRET=your-random-secret-key-here
```

### 3. ส่ง env เข้า container ใน docker-compose.yaml
```yaml
streamlit-dashboard:
  environment:
    - COOKIE_SECRET=${COOKIE_SECRET}
```

### 4. Rebuild และ restart
```bash
docker compose build streamlit-dashboard
docker compose up -d streamlit-dashboard
```