from .dwash.loader import DwashLoader
from .ftp.loader import FTPLoader
from df.common.exceptions import DFUnsupportedProviderException  # @UnresolvedImport


def get_loader(host, provider, token=None, **kwargs):
    provider_key = f'{provider.lower()}loader'
    loader_cls = {DwashLoader.__name__.lower(): DwashLoader,
                  FTPLoader.__name__.lower(): FTPLoader}.get(provider_key)  # @UndefinedVariable

    if not loader_cls:
        raise DFUnsupportedProviderException(f'Не найден лоадер для провайдера: {provider}')

    return loader_cls(host, token=token, **kwargs)
