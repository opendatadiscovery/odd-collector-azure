from oddrn_generator.generators import PowerBiGenerator
from odd_models.models import DataEntity, DataEntityType, DataConsumer, MetadataExtension
from odd_collector_azure.adapters.powerbi.domain.dataset import Dataset
from odd_collector_azure.adapters.powerbi import _METADATA_SCHEMA_URL_PREFIX


def map_dataset(
        oddrn_generator: PowerBiGenerator,
        dataset: Dataset,
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("datasets", dataset.name),
        name=dataset.name,
        type=DataEntityType.DATABASE_SERVICE,
        metadata=[MetadataExtension(
            schema_url=_METADATA_SCHEMA_URL_PREFIX,
            metadata={
                "id": dataset.id
            },
        )],
        owners=dataset.owner,
        data_consumer=DataConsumer(
            inputs=dataset.datasources
        ),
    )
