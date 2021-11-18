import logging
import os

from eodag import EODataAccessGateway

logger = logging.getLogger(__name__)


def get_product_by_id(
    product_id, out_dir, provider=None, config_file=None, product_type=None
):
    """
    Get satellite product with id using eodag
    :param product_id: id like S2A_MSIL1C_20200518T135121_N0209_R024_T21HTC_20200518T153019
    :param out_dir: Ouput directory
    :param provider: This is your data provider needed by eodag, could be different from the cloud provider
    :param config_file: Credentials for eodag, if none provided the credentials will be selected from env vars
    :param product_type: Product type, extra arg for eodag useful for creodias
    """
    if config_file is None:
        dag = EODataAccessGateway()
    else:
        dag = EODataAccessGateway(config_file)
    if provider is None:
        provider = os.getenv("EWOC_DATA_PROVIDER")
    dag.set_preferred_provider(provider)
    if product_type is not None:
        products, _ = dag.search(
            id=product_id, provider=provider, productType=product_type
        )
    else:
        products, _ = dag.search(id=product_id, provider=provider)
    if not products:
        logging.error("No results return by eodag!")
        raise ValueError
    dag.download(products[0], outputs_prefix=out_dir)
    # delete zip file
    list_out = os.listdir(out_dir)
    for item in list_out:
        if product_id in item and item.endswith("zip"):
            os.remove(os.path.join(out_dir, item))
