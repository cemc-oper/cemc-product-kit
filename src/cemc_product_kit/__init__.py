from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("cemc_product_kit")
except PackageNotFoundError:
    # package is not installed
    pass
