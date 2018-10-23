class CountryInfo(object):
    """
    Parameters
    ----------
    name : str or None
        The full name of the country, for example 'United States' or
        'China'.
    country_id : str
        The ISO 3166 alpha-2 country code where the exchange is located.
    country_id3 : str
        The ISO 3166 alpha-3 country code where the exchange is located.
    region: str
        The continent that the country belongs to
    """

    def __init__(self, name, country_id, country_id3, region):
        self.name = name
        self.country_id = country_id.upper()
        self.country_id3 = country_id3.upper()
        self.region = region

    def __repr__(self):
        return '%s(%r, %r, %r, %r)' % (
            type(self).__name__,
            self.name,
            self.country_id,
            self.country_id3,
            self.region,
        )        
