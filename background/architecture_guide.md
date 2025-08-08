# 🏗️ Background Tasks Architecture Guide - REGLAS CRÍTICAS

## 📋 **Router Pattern - ESTRUCTURA OBLIGATORIA**

### ✅ **Template Correcto para Router**

```python
@router.post(
    "/execute",
    response_model=TaskExecuteResponse,  # 🚨 OBLIGATORIO
    response_class=ORJSONResponse,
    summary="🚀 Ejecutar [Nombre]",
    description="Ejecuta [descripción] usando Celery",
)
async def execute_task():
    """🎯 Control remoto para ejecutar [Nombre] bajo demanda"""
    try:
        logger.info("🎮 API: Iniciando ejecución remota de [Nombre]...")

        # Instanciar el wrapper de Celery
        processor = SomeProcessor()

        # Ejecutar task
        result = await processor.run(params...)

        logger.info(f"✅ API: Task enviada exitosamente - ID: {result['task_id']}")

        # 🚨 OBLIGATORIO: TaskExecuteResponse
        return TaskExecuteResponse(
            success=True,
            task_id=result["task_id"],
            message="Task [Nombre] enviada a Celery exitosamente",
            task_name="toolbox.[nombre]",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ API: Error ejecutando [Nombre]: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error ejecutando task: {str(e)}")
```

### ❌ **Errores Comunes - NO HACER**

1. **NO retornar dict directamente**:

```python
# ❌ MAL
return {"success": False, "message": "error"}

# ✅ BIEN
raise HTTPException(status_code=400, detail="error")
return TaskExecuteResponse(...)
```

2. **NO olvidar response_model**:

```python
# ❌ MAL
@router.post("/execute", response_class=ORJSONResponse)

# ✅ BIEN
@router.post("/execute", response_model=TaskExecuteResponse, response_class=ORJSONResponse)
```

3. **NO usar BaseProcessor.format_task_response mal**:

```python
# ❌ MAL (en endpoint execute)
return BaseProcessor.format_task_response(task_id)

# ✅ BIEN (solo en endpoint status)
@router.get("/status/{task_id}")
async def get_status(task_id: str):
    return BaseProcessor.format_task_response(task_id)
```

## 📋 **Task Pattern - ESTRUCTURA CELERY**

### ✅ **Template Correcto para Task**

```python
@celery_app.task(name="toolbox.[nombre]", bind=True, max_retries=0)
def nombre_task(self, param1=None, param2=None):
    """
    🎯 [Descripción] usando Celery
    """
    repository_factory = create_repository_factory()

    try:
        logger.info(f"🚀 Iniciando [nombre] - Task ID: {self.request.id}")

        result = asyncio.run(_execute_business_logic(
            repository_factory=repository_factory,
            param1=param1,
            param2=param2,
            task_id=self.request.id
        ))

        logger.info(f"✅ Task completada exitosamente: {result}")
        return result

    except Exception as e:
        error_msg = f"❌ Error en [nombre]_task: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)
```

## 📋 **Processor Pattern - WRAPPER OPCIONAL**

### ✅ **Template Correcto para Processor**

```python
class SomeProcessor(BaseProcessor):
    def __init__(self):
        super().__init__(
            task_name="toolbox.[nombre]",
            description="[Descripción]"
        )

    async def run(self, param1=None, param2=None) -> dict:
        try:
            logger.info(f"🔄 SomeProcessor ejecutando task con parámetros: param1={param1}")

            # 🎯 Ejecutar Celery task
            task_result = some_task.delay(param1=param1, param2=param2)

            # 📊 Formatear respuesta usando BaseProcessor
            response = self.format_task_response(task_result.id)

            response.update({
                "task_name": self.task_name,
                "description": self.description,
                "parameters": {"param1": param1, "param2": param2}
            })

            logger.info(f"✅ Task iniciado exitosamente: {task_result.id}")
            return response

        except Exception as e:
            error_msg = f"❌ Error en SomeProcessor: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "message": error_msg,
                "task_name": self.task_name
            }
```

## 🚨 **REGLAS CRÍTICAS NO NEGOCIABLES**

1. **Router SIEMPRE retorna TaskExecuteResponse**
2. **Task SIEMPRE usa repository_factory (NO @inject)**
3. **Processor es OPCIONAL, solo wrapper**
4. **Status endpoint usa BaseProcessor.format_task_response**
5. **HTTPException para errores, NO dict**
6. **Logging consistente con emojis**
7. **Tags descriptivos en router**

## 📊 **Checklist de Migración**

-   [ ] Task creado con patrón correcto
-   [ ] Router con TaskExecuteResponse
-   [ ] Processor opcional implementado
-   [ ] Celery_config actualizado
-   [ ] **init**.py actualizados
-   [ ] main.py registra router
-   [ ] Schemas actualizados
-   [ ] Tests básicos
