#!/bin/bash
# 🚨 Script de Debugging para Servidor 2GB - VERSIÓN OPTIMIZADA

echo "=== 🔍 DEBUGGING ADELANTA TOOLBOX (SIN RQ WORKER) ==="
echo "Fecha: $(date)"
echo "Servidor: Amazon Linux 2023 (2GB RAM)"
echo ""

echo "🖥️  MEMORIA TOTAL DEL SISTEMA:"
free -h
echo ""

echo "📊 DISTRIBUCIÓN DE MEMORIA OPTIMIZADA:"
echo "Sistema + MySQL:      ~700MB"
echo "Web Container:        ~650MB  (+50MB extra)"
echo "Celery Worker:        ~950MB  (+150MB extra)"
echo "Redis:                ~250MB"
echo "Celery Beat:          ~100MB"
echo "Buffer disponible:    ~350MB"
echo ""

echo "🐳 CONTAINERS ACTUALES:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.MemUsage}}\t{{.CPUPerc}}"
echo ""

echo "📈 STATS EN TIEMPO REAL:"
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.Name}}"
echo ""

echo "🔥 ESTADO CELERY WORKER:"
docker exec celery_worker celery -A config.celery_config inspect active 2>/dev/null || echo "❌ Celery worker no responde"
docker exec celery_worker celery -A config.celery_config inspect stats 2>/dev/null | grep -E "(pool|memory|prefetch)" || echo "❌ No se pueden obtener stats"
echo ""

echo "🔄 REDIS MEMORIA:"
docker exec redis redis-cli info memory | grep -E "(used_memory_human|maxmemory_human)" || echo "❌ Redis no responde"
echo ""

echo "⚠️  PROCESOS QUE MÁS MEMORIA CONSUMEN:"
ps aux --sort=-%mem | head -10
echo ""

echo "💾 USO DE DISCO:"
df -h /
echo ""

echo "🔍 LOGS RECIENTES CELERY (últimas 20 líneas):"
docker logs celery_worker --tail 20
echo ""

echo "🚨 BÚSQUEDA DE ERRORES DE MEMORIA:"
docker logs celery_worker --tail 100 | grep -i -E "(memory|killed|oom|error|failed|exception)" || echo "✅ No se encontraron errores de memoria recientes"
echo ""

echo "🎯 MONITOREO ESPECÍFICO KPI:"
echo "Fecha inicio procesamiento: 2019-07-01"
echo "Fecha actual: $(date '+%Y-%m-%d')"
echo "Años de datos a procesar: $(($(date +%Y) - 2019))"
echo ""

echo "💡 RECOMENDACIONES INMEDIATAS:"
echo "1. Monitorear 'docker stats' en tiempo real durante procesamiento"
echo "2. Si memoria > 90%, el worker se reiniciará automáticamente"
echo "3. Verificar que chunk_size=2000 en bulk_insert está funcionando"
echo "4. Con 950MB disponibles para Celery, debería manejar datasets grandes"
echo ""
