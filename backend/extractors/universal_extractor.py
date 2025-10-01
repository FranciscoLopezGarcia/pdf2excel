"""Unified extraction pipeline selecting the correct bank parser."""

import inspect
import logging
import os
from typing import List, Sequence

import pandas as pd

from parser_factory import detect_bank, get_parser
from parsers.generic_parser import GenericParser
from pdf_reader import PDFReader

logger = logging.getLogger(__name__)

RawData = List


class UniversalBankExtractor:
    """Extracts transactions from any supported bank statement."""

    def __init__(self, reader: PDFReader | None = None) -> None:
        self.reader = reader or PDFReader()

    def extract_from_pdf(self, pdf_path: str, filename_hint: str = "") -> pd.DataFrame:
        raw_data, plain_text = self.reader.extract_all(pdf_path)
        filename = filename_hint or os.path.basename(pdf_path)

        bank_name = detect_bank(plain_text, filename)
        logger.info("Banco detectado %s para %s", bank_name, filename)

        parser = get_parser(bank_name)
        parser.reader = self.reader

        dataframe = self._run_parser(parser, pdf_path, raw_data, plain_text, filename)

        if dataframe is None or dataframe.empty:
            if bank_name != "GENERIC":
                logger.warning(
                    "Parser %s devolvio dataframe vacio. Se usa GenericParser como fallback",
                    bank_name,
                )
            generic = GenericParser()
            generic.reader = self.reader
            dataframe = self._run_parser(generic, pdf_path, raw_data, plain_text, filename, force_generic=True)

        if isinstance(dataframe, pd.DataFrame):
            return dataframe
        return pd.DataFrame()

    def _run_parser(
        self,
        parser,
        pdf_path: str,
        raw_data: RawData,
        plain_text: str,
        filename: str,
        force_generic: bool = False,
    ) -> pd.DataFrame | None:
        parse_fn = getattr(parser, "parse", None)
        if not callable(parse_fn):
            logger.error("Parser %s no tiene metodo parse", parser.__class__.__name__)
            return None

        def invoke(current_raw: RawData, current_text: str) -> pd.DataFrame | None:
            normalized_raw = self._normalize_raw(current_raw, current_text)

            signature = inspect.signature(parse_fn)
            param_names: Sequence[str] = [
                name for name in signature.parameters.keys() if name != "self"
            ]

            args: List = []
            kwargs: dict = {}

            if param_names:
                primary = param_names[0]
                if primary in {"pdf_path", "path"}:
                    args.append(pdf_path)
                else:
                    args.append(normalized_raw)

                for name in param_names[1:]:
                    if name == "filename":
                        kwargs[name] = filename
                    elif name in {"text", "raw_text"}:
                        kwargs[name] = current_text
                    elif name in {"raw_data", "data", "lines"}:
                        kwargs[name] = normalized_raw
                    elif name in {"pdf_path", "path"}:
                        kwargs[name] = pdf_path
            else:
                args.append(normalized_raw)

            try:
                return parse_fn(*args, **kwargs)
            except TypeError as exc:
                logger.debug("Firma inesperada para %s: %s", parser.__class__.__name__, exc)

            call_patterns = [
                ([normalized_raw], {"filename": filename}),
                ([normalized_raw], {}),
                ([pdf_path], {"text": current_text, "filename": filename}),
                ([pdf_path], {"text": current_text}),
            ]

            for pattern_args, pattern_kwargs in call_patterns:
                try:
                    return parse_fn(*pattern_args, **pattern_kwargs)
                except TypeError:
                    continue
                except Exception as exc:
                    logger.error(
                        "Parser %s fallo con error: %s",
                        parser.__class__.__name__,
                        exc,
                        exc_info=True,
                    )
                    break

            if not force_generic:
                logger.error("Parser %s no pudo procesar %s", parser.__class__.__name__, filename)
            return None

        result = invoke(raw_data, plain_text)

        if (
            not force_generic
            and self._should_retry_with_tables(parser, result)
        ):
            alt_raw, alt_text = self.reader.extract_all(pdf_path, prefer_tables=True)
            if alt_raw and alt_raw != raw_data:
                logger.info(
                    "Reintentando parser %s usando extraccion basada en tablas",
                    parser.__class__.__name__,
                )
                result = invoke(alt_raw, alt_text)

        return result

    def _should_retry_with_tables(self, parser, result) -> bool:
        if getattr(parser, "DISABLE_TABLE_FALLBACK", False):
            return False
        prefer_tables = getattr(parser, "PREFER_TABLES", False)
        if isinstance(result, pd.DataFrame):
            return prefer_tables and result.empty
        if result is None:
            return prefer_tables
        return False

    @staticmethod

    def _normalize_raw(raw_data: RawData, plain_text: str) -> RawData:
        if isinstance(raw_data, list) and raw_data:
            return raw_data
        if plain_text:
            return [line for line in plain_text.splitlines() if line.strip()]
        return []
