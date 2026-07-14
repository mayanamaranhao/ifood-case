"""
Configurações centrais do projeto.

Centraliza paths, nomes de schemas/tabelas e parâmetros de execução
para evitar strings mágicas espalhadas pelo código.

IMPORTANTE (Databricks Free Edition): não existe mais DBFS root nem
clusters custom — o storage de arquivos (landing zone) precisa ser um
Unity Catalog Volume, e as tabelas Bronze/Silver/Gold vivem como schemas
dentro do catalog do Unity Catalog (por padrão, `workspace`).
"""

from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class LakeConfig:
    """Paths e nomes lógicos de cada camada do Data Lake (medalhão)."""

    # Catalog do Unity Catalog. Na Free Edition, o catalog padrão
    # normalmente é "workspace" — confirme em Catalog na UI.
    catalog: str = "workspace"

    # Volume usado como landing zone (arquivos brutos, não tabulares).
    # Precisa existir previamente: Catalog > workspace > default > Volumes
    # > Create Volume, ex.: "ifood_case".
    volume_schema: str = "default"
    volume_name: str = "ifood_case"

    landing_path: str = field(init=False)

    bronze_db: str = "bronze"
    silver_db: str = "silver"
    gold_db: str = "gold"

    bronze_table: str = "yellow_taxi_trips"
    silver_table: str = "yellow_taxi_trips"

    def __post_init__(self):
        volume_root = f"/Volumes/{self.catalog}/{self.volume_schema}/{self.volume_name}"
        object.__setattr__(self, "landing_path", f"{volume_root}/landing/yellow_taxi")

    @property
    def bronze_table_fqn(self) -> str:
        """Nome totalmente qualificado (catalog.schema.table) da tabela Bronze."""
        return f"{self.catalog}.{self.bronze_db}.{self.bronze_table}"

    @property
    def silver_table_fqn(self) -> str:
        return f"{self.catalog}.{self.silver_db}.{self.silver_table}"

    def gold_table_fqn(self, table_short_name: str) -> str:
        return f"{self.catalog}.{self.gold_db}.{table_short_name}"


@dataclass(frozen=True)
class IngestionConfig:
    """Parâmetros do escopo de ingestão (períodos e fonte)."""

    base_url: str = (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        "yellow_tripdata_{year}-{month:02d}.parquet"
    )
    year: int = 2023
    months: List[int] = field(default_factory=lambda: [1, 2, 3, 4, 5])


# Colunas mínimas exigidas pelo case na camada de consumo (Silver/Gold).
# As demais colunas originais podem ser descartadas na transformação Silver.
REQUIRED_COLUMNS = [
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]

LAKE = LakeConfig()
INGESTION = IngestionConfig()
