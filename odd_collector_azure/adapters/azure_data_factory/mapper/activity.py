from odd_collector_sdk.utils.metadata import extract_metadata, DefinitionType
from odd_models import DataEntity, DataEntityType, DataTransformer
from oddrn_generator import AzureDataFactoryGenerator

from odd_collector_azure.adapters.azure_data_factory.domain import ADFActivity
from ..utils import ADFMetadataEncoder


def map_activity(
        oddrn_generator: AzureDataFactoryGenerator,
        activity: ADFActivity,
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("activities", activity.name),
        name=activity.name,
        type=DataEntityType.JOB,
        metadata=[
            extract_metadata("azure_data_factory",
                             activity,
                             DefinitionType.DATASET,
                             jsonify=True,
                             flatten=True,
                             json_encoder=ADFMetadataEncoder)
        ],
        data_transformer=DataTransformer(inputs=[], outputs=[])

    )
