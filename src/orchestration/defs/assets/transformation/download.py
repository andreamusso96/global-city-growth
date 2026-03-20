import zipfile
import gzip
import shutil
from pathlib import Path
from typing import List, Dict, Any, Tuple
import yaml
import dagster as dg
from datetime import datetime
import pooch

from ...resources.resources import IpumsAPIClientResource, StorageResource
from ...resources.ipums_api import IpumsAPIExtractType

def _unzip(zip_path: Path, dest_dir: Path) -> None:
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(dest_dir)

def _gunzip(src_gz: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(src_gz, "rb") as fin, open(dest, "wb") as fout:
        shutil.copyfileobj(fin, fout)

def _move(src: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dest))

def _make_temp_download_dir(base_dir: Path) -> Path:
    unique_id = str(datetime.now().timestamp())
    download_dir = base_dir / f"download_{unique_id}"
    download_dir.mkdir(parents=True, exist_ok=True)
    return download_dir


def _download_raw_data_zenodo(context: dg.AssetExecutionContext, storage: StorageResource, download_url: str) -> int:
    target_filename = "raw_data.zip"
    download_dir = _make_temp_download_dir(base_dir=storage.paths.download.download())
    zip_path = download_dir / target_filename

    downloader = pooch.HTTPDownloader(chunk_size=8*1024*1024, timeout=300, progressbar=True)
    
    context.log.info(f"Downloading {target_filename}. See stderr in dagster UI for progress.")
    downloaded_path = pooch.retrieve(
        url=download_url,               
        known_hash=None,               
        fname=target_filename,           
        path=str(download_dir),          
        downloader=downloader             
    )

    zip_path = Path(downloaded_path)
    context.log.info("Downloading finished")

    context.log.info(f"Unzipping {zip_path}")
    _unzip(zip_path=zip_path, dest_dir=download_dir)
    context.log.info(f"Unzipping finished")

    for p in download_dir.iterdir():
        if p.is_dir():
            _move(src=p, dest=storage.data_root)
    
    size = zip_path.stat().st_size
    shutil.rmtree(download_dir)
    return size

def _download_ipums_full_count_year(context: dg.AssetExecutionContext, storage: StorageResource, ipums_api_client: IpumsAPIClientResource, year: int, sample_id: str, variables: List[str], data_format: str) -> int:
    ipums = ipums_api_client.get_client()
    
    extract_id = ipums.submit_ipums_usa_extract_request(description=f"IPUMS USA Full Count {year}", sample=sample_id, variables=variables, data_format=data_format)
    context.log.info(f"Extract submitted with id {extract_id}")
    
    context.log.info(f"Waiting for extract to finish")
    download_url = ipums.wait_for_extract_request(extract_id=extract_id, extract_type=IpumsAPIExtractType.IPUM_USA_FULL_COUNT)
    context.log.info(f"Extract finished with id {extract_id}")

    context.log.info(f"Downloading extract")
    download_dir = _make_temp_download_dir(base_dir=storage.paths.download.download())
    ipums.download_extract(extract_id=extract_id, extract_type=IpumsAPIExtractType.IPUM_USA_FULL_COUNT, download_url=download_url, download_dir=download_dir, dagster_context=context)
    context.log.info(f"Extract downloaded")

    extract_id_formatted = f"{extract_id:05d}"
    gz_name = f"usa_{extract_id_formatted}.{data_format}.gz"
    gz_path = download_dir / gz_name

    final_path = storage.paths.usa.ipums_full_count.ipums_full_count(year=year)
    _gunzip(src_gz=gz_path, dest=final_path)

    shutil.rmtree(download_dir)
    return final_path.stat().st_size


def _download_nhgis_time_series(context: dg.AssetExecutionContext, storage: StorageResource, ipums_api_client: IpumsAPIClientResource, description: str, table_name: str, years: List[str], geog_level: str, final_csv: Path) -> int:
    ipums = ipums_api_client.get_client()
    
    context.log.info(f"Submitting extract")
    extract_id = ipums.submit_nhgis_time_series_extract_request(description=description, time_series_table=table_name, geog_level=geog_level, years=years, data_format="csv_header")
    context.log.info(f"Extract submitted with id {extract_id}")

    context.log.info(f"Waiting for extract to finish")
    download_url = ipums.wait_for_extract_request(extract_id=extract_id, extract_type=IpumsAPIExtractType.NHGIS_TIME_SERIES)
    context.log.info(f"Extract finished with id {extract_id}")

    context.log.info(f"Downloading extract")
    download_dir = _make_temp_download_dir(base_dir=storage.paths.download.download())
    ipums.download_extract(extract_id=extract_id, extract_type=IpumsAPIExtractType.NHGIS_TIME_SERIES, download_url=download_url, download_dir=download_dir)
    context.log.info(f"Extract downloaded")

    zip_name = f"nhgis{extract_id:04d}_csv.zip"
    zip_path = download_dir / zip_name

    _unzip(zip_path=zip_path, dest_dir=download_dir)

    matching_csvs = list(download_dir.rglob(f"nhgis*_ts_geog2010_{geog_level}.csv"))
    if len(matching_csvs) != 1:
        raise ValueError(f"Expected exactly one NHGIS time-series csv for geog level '{geog_level}', found {len(matching_csvs)}")

    source_csv = matching_csvs[0]
    if final_csv.exists():
        final_csv.unlink()
    _move(src=source_csv, dest=final_csv)

    shutil.rmtree(download_dir)
    return final_csv.stat().st_size

def _download_nhgis_shapefile(context: dg.AssetExecutionContext, storage: StorageResource, ipums_api_client: IpumsAPIClientResource, description: str, shapefile_name: str, final_dir: Path) -> int:
    ipums = ipums_api_client.get_client()

    context.log.info(f"Submitting extract")
    extract_id = ipums.submit_nhgis_shapefile_extract_request(description=description, shapefile_name=shapefile_name)
    context.log.info(f"Extract submitted with id {extract_id}")

    context.log.info(f"Waiting for extract to finish")
    download_url = ipums.wait_for_extract_request(extract_id=extract_id, extract_type=IpumsAPIExtractType.NHGIS_SHAPEFILE)
    context.log.info(f"Extract finished with id {extract_id}")

    context.log.info(f"Downloading extract")
    download_dir = _make_temp_download_dir(base_dir=storage.paths.download.download())
    ipums.download_extract(extract_id=extract_id, extract_type=IpumsAPIExtractType.NHGIS_SHAPEFILE, download_url=download_url, download_dir=download_dir)
    context.log.info(f"Extract downloaded")

    outer_zip_name = f"nhgis{extract_id:04d}_shape.zip"
    outer_zip_path = download_dir / outer_zip_name

    _unzip(zip_path=outer_zip_path, dest_dir=download_dir)

    inner_zip_candidates = [path for path in download_dir.rglob("*.zip") if path != outer_zip_path]
    if len(inner_zip_candidates) == 0:
        raise ValueError(f"No inner shapefile zip found for NHGIS shapefile '{shapefile_name}'")

    matching_inner_zips = [path for path in inner_zip_candidates if shapefile_name in path.name]
    if len(matching_inner_zips) == 1:
        inner_zip_path = matching_inner_zips[0]
    elif len(inner_zip_candidates) == 1:
        inner_zip_path = inner_zip_candidates[0]
    else:
        raise ValueError(f"Could not uniquely identify inner shapefile zip for '{shapefile_name}'")

    if final_dir.exists():
        shutil.rmtree(final_dir)
    final_dir.mkdir(parents=True, exist_ok=True)
    _unzip(zip_path=inner_zip_path, dest_dir=final_dir)
    shutil.rmtree(download_dir)
    return final_dir.stat().st_size


def _download_nhgis_1990_2020_place_population(context: dg.AssetExecutionContext, storage: StorageResource, ipums_api_client: IpumsAPIClientResource, table_name: str, years: List[str], geog_level: str) -> int:
    return _download_nhgis_time_series(
        context=context,
        storage=storage,
        ipums_api_client=ipums_api_client,
        description="NHGIS time series: place population 1990-2020",
        table_name=table_name,
        years=years,
        geog_level=geog_level,
        final_csv=storage.paths.usa.nhgis.census_place_pop_1990_2020(),
    )


def _download_nhgis_1900_2010_place_geom_year(context: dg.AssetExecutionContext, storage: StorageResource, ipums_api_client: IpumsAPIClientResource, year: int, shapefile_name: str) -> int:
    return _download_nhgis_shapefile(
        context=context,
        storage=storage,
        ipums_api_client=ipums_api_client,
        description=f"NHGIS Place Points {year}",
        shapefile_name=shapefile_name,
        final_dir=storage.paths.usa.nhgis.census_place_geom_folder(year=year),
    )


def _load_data_catalog(path: Path) -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def _get_inputs_raw_data_zenodo(data_catalog: Dict[str, Any]) -> str:
    zenodo_inputs = data_catalog["raw_data_zenodo"]
    download_url = zenodo_inputs["url"]
    return download_url

def _get_inputs_ipums_usa_full_count(data_catalog: Dict[str, Any]) -> Tuple[List[str], List[str], List[str], str]:
    ipums_inputs = data_catalog["ipums_usa_full_count"]
    extract_definitions = ipums_inputs["extract_definitions"]
    years = extract_definitions["years"]
    samples = extract_definitions["samples"]
    variables = extract_definitions["variables"]
    data_format = extract_definitions["data_format"]
    return years, samples, variables, data_format

def _get_inputs_nhgis_place_population_1990_2020(data_catalog: Dict[str, Any]) -> Tuple[str, List[str], str]:
    nhgis_inputs = data_catalog["nhgis_place_population_1990_2020"]
    extract_definitions = nhgis_inputs["extract_definitions"]
    table_name = extract_definitions["table_name"]
    years = extract_definitions["years"]
    geog_level = extract_definitions["geog_level"]
    return table_name, years, geog_level


def _get_inputs_nhgis_place_geom_1900_2010(data_catalog: Dict[str, Any]) -> Tuple[List[str], str]:
    nhgis_inputs = data_catalog["nhgis_place_geom_1900_2010"]
    extract_definitions = nhgis_inputs["extract_definitions"]
    years = extract_definitions["years"]
    shapefile_name_template = extract_definitions["shapefile_name_template"]
    return years, shapefile_name_template


def _get_inputs_nhgis_county_population_1990_2020(data_catalog: Dict[str, Any]) -> Tuple[str, List[str], str]:
    nhgis_inputs = data_catalog["nhgis_county_population_1990_2020"]
    extract_definitions = nhgis_inputs["extract_definitions"]
    table_name = extract_definitions["table_name"]
    years = extract_definitions["years"]
    geog_level = extract_definitions["geog_level"]
    return table_name, years, geog_level


def _get_inputs_nhgis_county_geom_2010(data_catalog: Dict[str, Any]) -> Tuple[List[str], str]:
    nhgis_inputs = data_catalog["nhgis_county_geom_2010"]
    extract_definitions = nhgis_inputs["extract_definitions"]
    years = extract_definitions["years"]
    shapefile_name_template = extract_definitions["shapefile_name_template"]
    return years, shapefile_name_template


def _get_inputs_nhgis_cbsa_geom_2010_2020(data_catalog: Dict[str, Any]) -> Tuple[List[str], str]:
    nhgis_inputs = data_catalog["nhgis_cbsa_geom_2010_2020"]
    extract_definitions = nhgis_inputs["extract_definitions"]
    years = extract_definitions["years"]
    shapefile_name_template = extract_definitions["shapefile_name_template"]
    return years, shapefile_name_template


@dg.asset(
    group_name="download",
)
def raw_data_zenodo(context: dg.AssetExecutionContext, storage: StorageResource):
    """A zip file called raw_data.zip containing all the raw data used in the project that are redistributable. See readme/README_RAW_DATA.md for more details."""

    context.log.info("Downloading Raw Data from Zenodo")
    data_catalog = _load_data_catalog(path=storage.data_catalog_path)
    download_url = _get_inputs_raw_data_zenodo(data_catalog=data_catalog)
    size = _download_raw_data_zenodo(context=context, storage=storage, download_url=download_url)
    return dg.Output(value=size, metadata={"size": size})

@dg.asset(
    group_name="download",
    deps=[raw_data_zenodo],
)
def ipums_usa_full_count_downloaded(context: dg.AssetExecutionContext, storage: StorageResource, ipums_api_client: IpumsAPIClientResource):
    """IPUMS USA Full Count data 1850-1940."""
    context.log.info("Downloading IPUMS USA Full Count")
    data_catalog = _load_data_catalog(path=storage.data_catalog_path)
    years, samples, variables, data_format = _get_inputs_ipums_usa_full_count(data_catalog=data_catalog)
    year_sample = zip(years, samples)
    total_size = 0
    for year, sample in year_sample:
        context.log.info(f"Downloading IPUMS USA Full Count {year} {sample}")
        size = _download_ipums_full_count_year(context=context, storage=storage, ipums_api_client=ipums_api_client, year=year, sample_id=sample, variables=variables, data_format=data_format)
        total_size += size

    return dg.Output(value=total_size, metadata={"size": total_size})

@dg.asset(
    group_name="download",
    deps=[raw_data_zenodo],
)
def nhgis_place_population_1990_2020_downloaded(context: dg.AssetExecutionContext, storage: StorageResource, ipums_api_client: IpumsAPIClientResource):
    """NHGIS Place Population 1990-2020."""
    context.log.info("Downloading NHGIS Place Population 1990-2020")
    data_catalog = _load_data_catalog(path=storage.data_catalog_path)
    table_name, years, geog_level = _get_inputs_nhgis_place_population_1990_2020(data_catalog=data_catalog)
    size = _download_nhgis_1990_2020_place_population(context=context, storage=storage, ipums_api_client=ipums_api_client, table_name=table_name, years=years, geog_level=geog_level)
    return dg.Output(value=size, metadata={"size": size})

@dg.asset(
    group_name="download",
    deps=[raw_data_zenodo],
)
def nhgis_place_geom_1900_2010_downloaded(context: dg.AssetExecutionContext, storage: StorageResource, ipums_api_client: IpumsAPIClientResource):
    """NHGIS Place Geometry 1900-2010."""
    context.log.info("Downloading NHGIS Place Geometry 1900-2010")
    data_catalog = _load_data_catalog(path=storage.data_catalog_path)
    years, shapefile_name_template = _get_inputs_nhgis_place_geom_1900_2010(data_catalog=data_catalog)
    shapefile_names = [shapefile_name_template.format(year=year) for year in years]
    year_shapefile_name = zip(years, shapefile_names)

    total_size = 0 
    for year, shapefile_name in year_shapefile_name:
        context.log.info(f"Downloading NHGIS Place Geometry {year} {shapefile_name}")
        size = _download_nhgis_1900_2010_place_geom_year(context=context, storage=storage, ipums_api_client=ipums_api_client, year=year, shapefile_name=shapefile_name)
        total_size += size

    return dg.Output(value=total_size, metadata={"size": total_size})


@dg.asset(
    group_name="download",
    deps=[raw_data_zenodo],
)
def nhgis_county_population_1990_2020_downloaded(context: dg.AssetExecutionContext, storage: StorageResource, ipums_api_client: IpumsAPIClientResource):
    """NHGIS County Population 1990-2020 standardized to 2010 geography."""
    context.log.info("Downloading NHGIS County Population 1990-2020")
    data_catalog = _load_data_catalog(path=storage.data_catalog_path)
    table_name, years, geog_level = _get_inputs_nhgis_county_population_1990_2020(data_catalog=data_catalog)
    size = _download_nhgis_time_series(
        context=context,
        storage=storage,
        ipums_api_client=ipums_api_client,
        description="NHGIS time series: county population 1990-2020",
        table_name=table_name,
        years=years,
        geog_level=geog_level,
        final_csv=storage.paths.usa.suburbanization.county_population(),
    )
    return dg.Output(value=size, metadata={"size": size})


@dg.asset(
    group_name="download",
    deps=[raw_data_zenodo],
)
def nhgis_county_geom_2010_downloaded(context: dg.AssetExecutionContext, storage: StorageResource, ipums_api_client: IpumsAPIClientResource):
    """NHGIS County Geometry 2010."""
    context.log.info("Downloading NHGIS County Geometry 2010")
    data_catalog = _load_data_catalog(path=storage.data_catalog_path)
    years, shapefile_name_template = _get_inputs_nhgis_county_geom_2010(data_catalog=data_catalog)
    total_size = 0
    for year in years:
        shapefile_name = shapefile_name_template.format(year=year)
        size = _download_nhgis_shapefile(
            context=context,
            storage=storage,
            ipums_api_client=ipums_api_client,
            description=f"NHGIS County Geometry {year}",
            shapefile_name=shapefile_name,
            final_dir=storage.paths.usa.suburbanization.county_geom_2010().parent,
        )
        total_size += size

    return dg.Output(value=total_size, metadata={"size": total_size})


@dg.asset(
    group_name="download",
    deps=[raw_data_zenodo],
)
def nhgis_cbsa_geom_2010_2020_downloaded(context: dg.AssetExecutionContext, storage: StorageResource, ipums_api_client: IpumsAPIClientResource):
    """NHGIS CBSA Geometry 2010 and 2020."""
    context.log.info("Downloading NHGIS CBSA Geometry 2010 and 2020")
    data_catalog = _load_data_catalog(path=storage.data_catalog_path)
    years, shapefile_name_template = _get_inputs_nhgis_cbsa_geom_2010_2020(data_catalog=data_catalog)
    total_size = 0
    for year in years:
        shapefile_name = shapefile_name_template.format(year=year)
        size = _download_nhgis_shapefile(
            context=context,
            storage=storage,
            ipums_api_client=ipums_api_client,
            description=f"NHGIS CBSA Geometry {year}",
            shapefile_name=shapefile_name,
            final_dir=storage.paths.usa.suburbanization.cbsa_geom(year=int(year)).parent,
        )
        total_size += size

    return dg.Output(value=total_size, metadata={"size": total_size})
