from metacatalog import models
from metacatalog.api import add as api


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
    variable=models.Variable
)

ADD_MAPPING = dict(
    license=api.add_license,
    licenses=api.add_license,
    keyword=api.add_keyword,
    keywords=api.add_keyword,
    unit=api.add_unit,
    units=api.add_unit,
    variable=api.add_variable,
    variables=api.add_variable,
    person=api.add_person,
    persons=api.add_person,
    author=api.add_person,
    contributors=api.add_person,
    entry=api.add_entry,
    entries=api.add_entry,
)
