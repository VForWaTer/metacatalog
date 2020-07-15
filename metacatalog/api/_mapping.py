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
    variables=models.Variable,
    thesaurus=models.Thesaurus,
    entry_groups=models.EntryGroup,
)

ENTITY_MAPPING = dict(
    datasourcetype=models.DataSourceType,
    datasource_type=models.DataSourceType,
    datasource=models.DataSource,
    entry=models.Entry,
    keyword=models.Keyword,
    license=models.License,
    person=models.Person,
    author=models.Person,
    contributor=models.Person,
    person_role=models.PersonRole,
    personrole=models.PersonRole,
    unit=models.Unit,
    variable=models.Variable,
    thesaurus=models.Thesaurus,
    thesaurusName=models.Thesaurus
)
