"""Parser factory - Detecta banco y retorna el parser apropiado."""

import logging
from typing import Dict, Type
import pandas as pd

from parsers.generic_parser import GenericParser
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
    """Obtiene instancia de parser por nombre de banco."""
    parser_cls = _PARSERS.get(bank_name.upper(), GenericParser)
    return parser_cls()


def available_parsers() -> Dict[str, Type]:
    """Lista todos los parsers disponibles."""
    return dict(_PARSERS)


def detect_bank(text: str, filename: str = "") -> str:
    """
    Detecta el banco analizando texto y nombre de archivo.
    
    Args:
        text: Texto extraído del PDF
        filename: Nombre del archivo (opcional)
    
    Returns:
        Nombre del banco detectado o "GENERIC"
    """
    haystack_text_upper = (text or "").upper()
    haystack_file_upper = (filename or "").upper()

    for bank, parser_cls in _PARSERS.items():
        if bank == "GENERIC":
            continue

        parser = parser_cls()
        
        # Intentar método detect() personalizado
        detect_method = getattr(parser, "detect", None)
        if callable(detect_method):
            try:
                if detect_method(text, filename):
                    logger.info(f"✅ Banco detectado: {bank} (método detect)")
                    return bank
            except Exception as exc:
                logger.debug(f"Detection method failed for {bank}: {exc}")

        # Fallback a keywords
        keywords = getattr(parser, "DETECTION_KEYWORDS", None) or getattr(parser, "KEYWORDS", None)
        if keywords:
            for keyword in keywords:
                keyword_upper = str(keyword).upper()
                if keyword_upper and (
                    keyword_upper in haystack_text_upper or 
                    keyword_upper in haystack_file_upper
                ):
                    logger.info(f"✅ Banco detectado: {bank} (keyword: {keyword})")
                    return bank

    logger.warning("⚠️ No se detectó banco específico, usando GENERIC")
    return "GENERIC"


def parse_pdf(pdf_path: str) -> pd.DataFrame:
    """
    Flujo completo: extrae y parsea un PDF bancario.
    
    Este es el punto de entrada principal que deberías usar.
    
    Args:
        pdf_path: Ruta al archivo PDF
    
    Returns:
        DataFrame con transacciones parseadas
    
    Raises:
        ValueError: Si no se pudo extraer datos del PDF
    """
    from extractors.universal_extractor import UniversalBankExtractor
    
    # 1. Extraer datos crudos
    logger.info(f"📂 Procesando: {pdf_path}")
    extractor = UniversalBankExtractor()
    raw_data = extractor.extract_from_pdf(pdf_path)
    
    if not raw_data['text'] and not raw_data['tables']:
        raise ValueError(f"No se pudo extraer datos de {pdf_path}")
    
    logger.info(f"✅ Extracción exitosa: método={raw_data['method']}, "
                f"texto={len(raw_data['text'])} chars, "
                f"tablas={len(raw_data['tables'])}")
    
    # 2. Detectar banco
    bank_name = detect_bank(raw_data['text'], pdf_path)
    
    # 3. Obtener parser apropiado
    parser = get_parser(bank_name)
    logger.info(f"🔧 Usando parser: {parser.BANK_NAME}")
    
    # 4. Parsear
    try:
        df = parser.parse(raw_data, filename=pdf_path)
        logger.info(f"✅ Parsing exitoso: {len(df)} transacciones")
        return df
    except Exception as e:
        logger.error(f"❌ Error en parsing con {bank_name}: {e}")
        
        # Fallback a genérico si el específico falla
        if bank_name != "GENERIC":
            logger.info("🔄 Intentando con GenericParser...")
            generic = GenericParser()
            return generic.parse(raw_data, filename=pdf_path)
        raise