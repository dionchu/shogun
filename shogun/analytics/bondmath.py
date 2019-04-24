import QuantLib as ql

def billprice(discount_rate, trade_date, maturity_date, issue_date):
    maturity_date = ql.DateParser.parseFormatted(maturity_date,'%Y-%m-%d')
    trade_date = ql.DateParser.parseFormatted(trade_date,'%Y-%m-%d')
    issue_date = ql.DateParser.parseFormatted(issue_date,'%Y-%m-%d')
    settle_date = max(ql.UnitedStates(ql.UnitedStates.GovernmentBond).advance(trade_date,1,ql.Days), issue_date)
    return round(100 * (1 - discount_rate * (maturity_date - settle_date)/360),5)

def billdiscount(price, trade_date, maturity_date, issue_date):
    maturity_date = ql.DateParser.parseFormatted(maturity_date,'%Y-%m-%d')
    trade_date = ql.DateParser.parseFormatted(trade_date,'%Y-%m-%d')
    issue_date = ql.DateParser.parseFormatted(issue_date,'%Y-%m-%d')
    settle_date = max(ql.UnitedStates(ql.UnitedStates.GovernmentBond).advance(trade_date,1,ql.Days), issue_date)
    return round((1-price/100) / ((maturity_date - settle_date)/360) ,5)

def bndprice(ytm, coupon, trade_date, maturity_date, issue_date):

    trade_date = ql.DateParser.parseFormatted(trade_date,'%Y-%m-%d')
    ql.Settings.instance().evaluationDate = trade_date

    maturity_date = ql.DateParser.parseFormatted(maturity_date,'%Y-%m-%d')
    issue_date = ql.DateParser.parseFormatted(issue_date,'%Y-%m-%d')

    accrual_basis = ql.ActualActual(ql.ActualActual.Bond)
    coupon_freq = ql.Semiannual
    period = ql.Period(coupon_freq)
    compounding = ql.Compounded
    calendar = ql.UnitedStates(ql.UnitedStates.GovernmentBond)
    convention = ql.Following
    termination_convention = convention
    end_of_month = True
    rule = ql.DateGeneration.Backward

    schedule = ql.Schedule(issue_date,
                           maturity_date,
                           period,
                           calendar,
                           convention,
                           termination_convention,
                           rule,
                           end_of_month)

    settlement_days = 1
    face_amount = 100
    redemption = 100
    coupon = [coupon] if not isinstance(coupon, list) else coupon

    bond = FixedRateBond(settlement_days, redemption, schedule, coupon, accrual_basis)
    return round(bond.cleanPrice(ytm, accrual_basis, compounding, coupon_freq),5)

def bndyield(price, coupon, trade_date, maturity_date, issue_date):

    trade_date = ql.DateParser.parseFormatted(trade_date,'%Y-%m-%d')
    ql.Settings.instance().evaluationDate = trade_date

    maturity_date = ql.DateParser.parseFormatted(maturity_date,'%Y-%m-%d')
    issue_date = ql.DateParser.parseFormatted(issue_date,'%Y-%m-%d')

    accrual_basis = ql.ActualActual(ql.ActualActual.Bond)
    coupon_freq = ql.Semiannual
    period = ql.Period(coupon_freq)
    compounding = ql.Compounded
    calendar = ql.UnitedStates(ql.UnitedStates.GovernmentBond)
    convention = ql.Following
    termination_convention = convention
    end_of_month = True
    rule = ql.DateGeneration.Backward

    schedule = ql.Schedule(issue_date,
                           maturity_date,
                           period,
                           calendar,
                           convention,
                           termination_convention,
                           rule,
                           end_of_month)

    settlement_days = 1
    face_amount = 100
    redemption = 100
    coupon = [coupon] if not isinstance(coupon, list) else coupon

    bond = FixedRateBond(settlement_days, redemption, schedule, coupon, accrual_basis)
    return round(bond.bondYield(price, accrual_basis, compounding, coupon_freq),4)
