'''
Created on 11 Jul 2019

@author: pymancer


Иерархия исключений DF
Здесь должны быть только общие исключения, т.е вызываемые в разных исполнителях,
чтобы из-за появления новых исполнителей не приходилось менять всю иерархию.
'''


class DFException(Exception):
    pass


class DFOperatorException(DFException):
    pass


class DFExtractorException(DFOperatorException):
    pass


class DFUnsupportedEntityException(DFExtractorException):
    pass


class DFAdapterException(DFOperatorException):
    pass


class DFPIPwareHookException(DFAdapterException):
    pass


class DFEBWebHookException(DFAdapterException):
    pass


class DFKZStatHookException(DFAdapterException):
    pass


class DFKZTaskHookException(DFAdapterException):
    pass


class DFNasdaqHtmlWebHookException(DFAdapterException):
    pass


class DFLoaderException(DFOperatorException):
    pass


class DFUnsupportedProviderException(DFLoaderException):
    pass


class DFQueryBuilderException(DFOperatorException):
    pass


class DFUploaderException(DFOperatorException):
    pass


class DFViHookException(DFOperatorException):
    pass


class DFViAPIException(DFOperatorException):
    pass
