from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer

from metacatalog.db.base import Base


class EddyData(Base):
    __tablename__ = 'eddy_data'

    # columns
    entry_id  = Column(Integer, ForeignKey('entries.id'), primary_key=True)

    # TODO: Datentypen überprüfuen, welche Werte sind nicht nullable? Gibt es weitere keys?

    t_begin = Column(DateTime, nullable = False)
    t_end = Column(DateTime, nullable = False)
    u = Column(Numeric)
    v = Column(Numeric)
    w = Column(Numeric)
    ts = Column(Numeric)
    tp = Column(Numeric)
    a = Column(Numeric)
    co2 = Column(Numeric)
    t_ref = Column(Numeric)
    a_ref = Column(Numeric)
    p_ref = Column(Numeric)
    var_u = Column(Numeric)
    var_v = Column(Numeric)
    var_w = Column(Numeric)
    var_ts = Column(Numeric)
    var_tp = Column(Numeric)
    var_a = Column(Numeric)
    var_co2 = Column(Numeric)
    cov_u_v = Column(Numeric)
    cov_v_w = Column(Numeric)
    cov_u_w = Column(Numeric)
    cov_u_ts = Column(Numeric)
    cov_v_ts = Column(Numeric)
    cov_w_ts = Column(Numeric)
    cov_u_tp = Column(Numeric)
    cov_v_tp = Column(Numeric)
    cov_w_tp = Column(Numeric)
    cov_u_a = Column(Numeric)
    cov_v_a = Column(Numeric)
    cov_w_a = Column(Numeric)
    cov_u_co2 = Column(Numeric)
    cov_v_co2 = Column(Numeric)
    cov_w_co2 = Column(Numeric)
    nvalue = Column(Integer)
    dir = Column(Numeric)
    ustar = Column(Numeric)
    hts = Column(Numeric)
    htp = Column(Numeric)
    lve = Column(Numeric)
    zl = Column(Numeric)
    zl_v = Column(Numeric)
    flag_ustar = Column(Integer)        # ist in Daten 0, 1 oder 2 -> Integer? Oder kann das auch andere Werte annehmen?
    flag_hts = Column(Integer)          # ist in Daten 0, 1 oder 2 -> Integer? Oder kann das auch andere Werte annehmen?
    flag_htp = Column(Integer)          # ist in Daten 0, 1 oder 2 -> Integer? Oder kann das auch andere Werte annehmen?
    flag_lve = Column(Integer)          # ist in Daten 0, 1 oder 2 -> Integer? Oder kann das auch andere Werte annehmen?
    flag_wco = Column(Integer)          # ist in Daten 0, 1 oder 2 -> Integer? Oder kann das auch andere Werte annehmen?
    t_mid = Column(DateTime)
    fcstor = Column(Numeric)
    nee = Column(Numeric)
    footprint_trgt_1 = Column(Numeric)
    footprint_trgt_2 = Column(Numeric)
    footprnt_xmax = Column(Numeric)
    r_err_ustar = Column(Numeric)
    r_err_hts = Column(Numeric)
    r_err_lve = Column(Numeric)
    r_err_co2 = Column(Numeric)
    noise_ustar = Column(Numeric)
    noise_hts = Column(Numeric)
    noise_lve = Column(Numeric)
    noise_co2 = Column(Numeric)
