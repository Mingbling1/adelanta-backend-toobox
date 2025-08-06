#!/bin/bash
# 🔍 Script de Monitoreo de Memoria para Servidor 2GB

echo "=== 📊 MONITOREO DE MEMORIA ADELANTA TOOLBOX ==="
echo "Fecha: $(date)"
echo ""

echo "🖥️  MEMORIA DEL SISTEMA:"
free -h
echo ""

echo "🐳 MEMORIA DE CONTAINERS:"
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.Name}}"
echo ""

echo "📦 CONTAINERS EJECUTÁNDOSE:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""

echo "🔍 PROCESOS TOP MEMORIA:"
ps aux --sort=-%mem | head -10
echo ""

echo "📈 USO DE DISCO:"
df -h
echo ""

echo "🔥 CELERY WORKERS:"
docker exec celery_worker celery -A config.celery_config inspect active 2>/dev/null || echo "Celery worker no responde"
echo ""

echo "🔄 REDIS INFO:"
docker exec redis redis-cli info memory | grep used_memory_human
echo ""
