from metacatalog import models


TABLE_MAPPING = dict(
    datasource_types=models.datasource.DataSourceType,
    datasources=models.datasource.DataSource,
    entries=models.entry.Entry,
    keywords=models.keyword.Keyword,
    licenses=models.license.License,
    persons=models.person.Person,
    person_roles=models.person.PersonRole,
    units=models.variable.Unit,
    variables=models.variable.Variable
)