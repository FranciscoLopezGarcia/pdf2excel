"""Parser factory."""

import logging
from typing import Dict, Type

from parsers.generic_parser import GenericParser
from parsers.bbva import BBVAParser
from parsers.bpn import BPNParser
from parsers.ciudad import CiudadParser
from parsers.comafi import ComafiParser
from parsers.credicoop import CredicoopParser
from parsers.galicia import GaliciaParser
from parsers.galicia_mas import GaliciaMasParser
from parsers.hipotecario import HipotecarioParser
from parsers.hsbc import HSBCParser
from parsers.icbc import ICBCParser
from parsers.itau import ItauParser
from parsers.macro import MacroParser
from parsers.mercadopago import MercadoPagoParser
from parsers.nacion import NacionParser
from parsers.patagonia import PatagoniaParser
from parsers.provincia import ProvinciaParser
from parsers.rioja import RiojaParser
from parsers.sanjuan import SanJuanParser
from parsers.santander import SantanderParser
from parsers.supervielle import SupervielleParser
from parsers.supervielle_USD import SupervielleUSDParser

logger = logging.getLogger(__name__)

_PARSERS: Dict[str, Type] = {
    # Specific variants before broader handlers
    "SUPERVIELLE_USD": SupervielleUSDParser,
    "SUPERVIELLE": SupervielleParser,
    "GALICIA_MAS": GaliciaMasParser,
    "GALICIA": GaliciaParser,
    "ITAU": ItauParser,
    "MACRO": MacroParser,
    "COMAFI": ComafiParser,
    "BPN": BPNParser,
    "RIOJA": RiojaParser,
    "HIPOTECARIO": HipotecarioParser,
    "MERCADOPAGO": MercadoPagoParser,
    "SANTANDER": SantanderParser,
    "NACION": NacionParser,
    "PROVINCIA": ProvinciaParser,
    "SAN_JUAN": SanJuanParser,
    "PATAGONIA": PatagoniaParser,
    "BBVA": BBVAParser,
    "ICBC": ICBCParser,
    "CIUDAD": CiudadParser,
    "CREDICOOP": CredicoopParser,
    "HSBC": HSBCParser,
    "GENERIC": GenericParser,
}


def get_parser(bank_name: str):
    parser_cls = _PARSERS.get(bank_name.upper(), GenericParser)
    return parser_cls()


def available_parsers() -> Dict[str, Type]:
    return dict(_PARSERS)


def detect_bank(text: str, filename: str = "") -> str:
    haystack_text = (text or "")
    haystack_text_upper = haystack_text.upper()
    haystack_file_upper = (filename or "").upper()

    for bank, parser_cls in _PARSERS.items():
        if bank == "GENERIC":
            continue

        parser = parser_cls()
        detect_method = getattr(parser, "detect", None)
        if callable(detect_method):
            try:
                if detect_method(haystack_text, filename):
                    return bank
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("Detection failed for %s with %s", bank, exc)

        keywords = getattr(parser, "DETECTION_KEYWORDS", None) or getattr(parser, "KEYWORDS", None)
        if keywords:
            for keyword in keywords:
                keyword_upper = str(keyword).upper()
                if keyword_upper and (
                    keyword_upper in haystack_text_upper or keyword_upper in haystack_file_upper
                ):
                    return bank

    return "GENERIC"
