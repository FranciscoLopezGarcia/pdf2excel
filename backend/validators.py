import logging

logger = logging.getLogger(__name__)

def validate_dataframe(df):
    if df.empty:
        logger.warning("⚠️ DataFrame vacío")
        return

    # Check 1: saldos negativos
    if (df["saldo"].astype(float) < 0).any():
        logger.warning("⚠️ Hay saldos negativos")

    # Check 2: filas con debito=credito=0 y detalle no vacío
    mask = (df["debito"].astype(float) == 0) & (df["credito"].astype(float) == 0) & (df["detalle"] != "")
    if mask.any():
        logger.warning(f"⚠️ {mask.sum()} filas con debito=0 y credito=0")

    # Check 3: integridad de saldos (saldo anterior vs calculado)
    try:
        saldo_calc = df["saldo"].astype(float).shift(1) - df["debito"].astype(float) + df["credito"].astype(float)
        inconsistencias = (df["saldo"].astype(float) != saldo_calc).sum()
        if inconsistencias > 0:
            logger.warning(f"⚠️ {inconsistencias} inconsistencias en saldos detectadas")
    except Exception:
        logger.debug("No se pudo validar integridad de saldos (faltan datos)")
