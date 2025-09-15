#!/bin/bash
echo "🔍 Health Check..."

# Check MySQL
docker exec sleman_mysql mysql -u appuser -papppass123 -e "SELECT 1" > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "✅ MySQL: OK"
else
    echo "❌ MySQL: FAILED"
fi

# Check Metabase
curl -f http://localhost:3000 > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "✅ Metabase: OK"
else
    echo "❌ Metabase: FAILED"
fi

# Check Frontend
curl -f http://localhost:3001 > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "✅ Frontend: OK"
else
    echo "❌ Frontend: FAILED"
fi