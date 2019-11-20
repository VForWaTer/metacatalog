from metacatalog import models


TABLE_MAPPING = dict(
    datasource_types=models.DataSourceType,
    datasources=models.DataSource,
    entries=models.Entry,
    keywords=models.Keyword,
    licenses=models.License,
    persons=models.Person,
    person_roles=models.PersonRole,
    units=models.Unit,
    variables=models.Variable
)