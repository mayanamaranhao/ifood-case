"""Baixa os arquivos parquet originais da NYC TLC para a landing zone.

Landing zone = pouso dos arquivos EXATAMENTE como publicados pela fonte,
sem nenhuma transformação. É o nosso ponto de reprocessamento: se algo
der errado nas camadas seguintes, reprocessamos a partir daqui sem
precisar bater na fonte externa de novo.

Databricks Free Edition: Unity Catalog Volumes são montados como paths
POSIX normais (`/Volumes/<catalog>/<schema>/<volume>/...`), acessíveis
tanto do driver quanto de qualquer biblioteca Python padrão — por isso
baixamos direto para `LAKE.landing_path`, sem staging intermediário nem
`dbutils.fs.cp`.

LIMITAÇÃO CONHECIDA: a Free Edition restringe acesso de rede de saída a
uma lista de domínios confiáveis não documentada publicamente. Se a
CloudFront da NYC TLC (`d37ci6vzurychx.cloudfront.net`) não estiver
liberada, este script falha com erro de conexão — nesse caso, o plano B
é baixar os 5 arquivos manualmente na sua máquina e fazer upload direto
pela UI (Catalog > Volume > Upload to this volume) para o mesmo path de
`LAKE.landing_path`.
"""

from __future__ import annotations

import urllib.request
from pathlib import Path
from typing import Iterator

from src.config import INGESTION, LAKE
from src.common.utils import get_logger

logger = get_logger(__name__)


def _iter_source_urls() -> Iterator[tuple[int, int, str]]:
    """Gera (ano, mes, url) para cada mês configurado em INGESTION."""
    for month in INGESTION.months:
        url = INGESTION.base_url.format(year=INGESTION.year, month=month)
        yield INGESTION.year, month, url


def download_month(year: int, month: int, url: str, dest_dir: Path) -> Path:
    """Baixa um único arquivo mensal, se ainda não existir na landing zone.

    Idempotente: reexecutar o pipeline não baixa de novo um arquivo já
    presente, permitindo re-runs baratos.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_file = dest_dir / f"yellow_tripdata_{year}-{month:02d}.parquet"

    if dest_file.exists():
        logger.info("Já existe, pulando download: %s", dest_file.name)
        return dest_file

    logger.info("Baixando %s -> %s", url, dest_file)
    try:
        urllib.request.urlretrieve(url, dest_file)
    except Exception as exc:
        logger.error(
            "Falha ao baixar %s (%s). Se for erro de rede/domínio bloqueado, "
            "faça upload manual do arquivo para %s via UI do Databricks "
            "(Catalog > Volume > Upload to this volume).",
            url,
            exc,
            dest_dir,
        )
        raise
    return dest_file


def run() -> list[Path]:
    """Orquestra o download de todos os meses configurados direto para a
    landing zone (Unity Catalog Volume)."""
    landing_dir = Path(LAKE.landing_path)
    downloaded_files = [
        download_month(year, month, url, landing_dir)
        for year, month, url in _iter_source_urls()
    ]
    logger.info("Download concluído: %d arquivo(s).", len(downloaded_files))
    return downloaded_files


if __name__ == "__main__":
    run()
