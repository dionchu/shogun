class FinancialCenterInfo(object):
    """A financial center is a hub of financial markets
    Parameters
    ----------
    name : str or None
        The full name of the financial center, for example 'New_York' or
        'London'.
    country_info : CountryInfo object
        Country object with identifying information.
    timezone: str
        The timezone of the financial center using the tz database specifications
    """

    def __init__(self, name, country_info, timezone):
        self.name = name
        self.country_info = country_info
        self.timezone = timezone

    def __repr__(self):
        return '%s(%r, %r, %r, %r, %r)' % (
            type(self).__name__,
            self.name,
            self.country_id,
            self.country_id3,
            self.region,
            self.timezone,
        )

    @property
    def country_id(self):
        return self.country_info.country_id

    @property
    def country_id3(self):
        return self.country_info.country_id3

    @property
    def region(self):
        return self.country_info.region
