from repositories.datamart.NuevosClientesNuevosPagadoresRepository import (
    NuevosClientesNuevosPagadoresRepository,
)
from fastapi import Depends


class NuevosClientesNuevosPagadoresService:
    def __init__(
        self,
        nuevos_clientes_nuevos_pagadores_repository: NuevosClientesNuevosPagadoresRepository = Depends(),
    ):
        self.nuevos_clientes_nuevos_pagadores_repository = (
            nuevos_clientes_nuevos_pagadores_repository
        )

    async def create_many(self, input: list[dict]):
        await self.nuevos_clientes_nuevos_pagadores_repository.create_many(input)

    async def delete_all(self):
        await self.nuevos_clientes_nuevos_pagadores_repository.delete_all()
