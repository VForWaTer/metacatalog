from typing import Union


from lxml import etree
import xmltodict


from metacatalog.ext import MetacatalogExtensionInterface
from metacatalog.ext.standard_export.util import _parse_iso_information, _init_iso19115_jinja, _validate_xml
from metacatalog import api
from metacatalog.models import Entry, Person
from metacatalog.util.results import ImmutableResultSet


class StandardsExportExtension(MetacatalogExtensionInterface):
    r"""
    Extension to export Entries in standard format.
    Currently, ISO 19115 export is implemented.
    """
    @classmethod
    def init_extension(cls):
        # wrapper which calls StandardsExportExtension.iso19115_export
        def wrapper(self: Entry, config_dict: dict, path: str = None):  
            return StandardsExportExtension.iso19115_export(entry_or_resultset=self, config_dict=config_dict, path=path)
        
        # iso19115_export docstring and name for wrapper function
        wrapper.__doc__ = StandardsExportExtension.iso19115_export.__doc__
        wrapper.__name__ = StandardsExportExtension.iso19115_export.__name__
        
        Entry.export_iso19115 = wrapper


    @classmethod
    def iso19115_export(cls, entry_or_resultset: Union[Entry, ImmutableResultSet], config_dict: dict, path: str = None):
        """
        Export a :class:`Entry <metacatalog.models.Entry>` or 
        :class:`ImmutableResultSet <metacatalog.util.results.ImmutableResultSet> to XML in 
        ISO 19115 standard.
        Repeatable information input is always a list, as we can loop over the lists in the
        jinja ISO 19115 template.
        If a path is given, a new XML file will be created.

        Parameters
        ----------
        entry_or_resultset : Union[Entry, ImmutableResultSet]
            The entry instance to be exported
        config_dict : dict
            Configuration dictionary, containing information about the data provider
            - contact
        path : str
            If given, a file location for export.
        
        Returns
        ----------
        out : str
            The XML str if path is None, else None

        Notes
        -----
        The content of the file will be created using a 
        :class:`ImmutableResultSet <metacatalog.utils.results.ImmutableResultSet>`.
        This will lazy-load sibling Entries and parent groups as needed for
        a useful Metadata export.

        """
        # get necessary input parameters from ImmutableResultSet for ISO export
        iso_input = _parse_iso_information(entry_or_resultset)

        # get initialized jinja template
        template = _init_iso19115_jinja()

        # render template with entry_dict
        xml = template.render(**iso_input, **config_dict)

        # check whether xml is well-formed
        _validate_xml(xml)

        # check path settings
        if path is None:
            return xml
            
        else:
            xml = etree.fromstring(xml) # this also checks whether xml is well-formed
            xml = etree.tostring(xml) # converts to binary string, necessary to write xml

            with open(path, 'w') as f:
                f.write(xml.decode(encoding='utf8'))


    @classmethod
    def iso_xml_import(xml, session, commit=True):
        """
        Import an ISO 19115 XML file and convert to an Entry with the 
        metacatalog metadata model.
        If a metacatalog session is passed, the Entry is directly imported
        to the database.
        
        Parameters
        ----------
        xml : str
            Path to XML file holding a ISO 19115-2 metadata record.
            
        session : 
            
        commit : bool
            If commit is True, the Entry is directly created in the passed
            metacatalog session. If False, the metadata dictionary is returned,
            which can be checked, the Entry can then be created with 
            `Entry.from_dict()`.

        no_data : 
            

        Returns
        -------
        out : 

        Notes
        -----
        The content of the file will be created using a 
        :class:`ImmutableResultSet <metacatalog.utils.results.ImmutableResultSet>`.
        This will lazy-load sibling Entries and parent groups as needed for
        an useful Metadata export.
        """
        # get xml metadata as dict
        with open(xml, "rb") as f:
            metadata = xmltodict.parse(f)

        # check metadata ISO 19115 compliance
        if metadata.keys().__str__() == "dict_keys(['gmi:MI_Metadata'])":
            metadata = metadata.get('gmi:MI_Metadata', {})
        elif metadata.keys().__str__() == "dict_keys(['gmi:MD_Metadata'])":
            metadata = metadata.get('gmi:MD_Metadata', {})
            raise Warning("ISO 19115-2 root element should be 'gmi:MI_Metadata', 'gmi:MD_Metadata' is also accepted but it is not guaranteed that the import will work.")
        else:
            raise ValueError("ISO 19115-2 root element must be either 'gmi:MI_Metadata' or 'gmi:MD_Metadata'")
        # we could also check <gmd:metadataStandardVersion>, but the root key is better for that I guess


        # uuid
        uuid = metadata.get('gmd:fileIdentifier', {}).get('gco:CharacterString')
        if api.find_entry(session, uuid=uuid):
            raise ValueError(f"The Entry with the uuid {uuid} already exists in the database session.")

        # title
        title = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:citation', {}).get('gmd:CI_Citation', {}).get('gmd:title', {}).get('gco:CharacterString')

        # author and coauthors
        authors = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:pointOfContact', [])

        coauthors_list = []

        for author in authors:
            role = author.get('gmd:CI_ResponsibleParty', {}).get('gmd:role', {}).get('gmd:CI_RoleCode', {}).get('#text')
            if role == 'author':
                fullname = author.get('gmd:CI_ResponsibleParty', {}).get('gmd:individualName', {}).get('gco:CharacterString')
                if fullname:
                    first_name = fullname.split(', ')[0]
                    last_name = fullname.split(', ')[1]
                else:
                    raise ValueError("No information about author first and last name provided.")
        
                organisation_name = author.get('gmd:CI_ResponsibleParty', {}).get('gmd:organisationName', {}).get('gco:CharacterString')

                author_dict = dict(
                    first_name = first_name,
                    last_name = last_name,
                    organisation_name = organisation_name
                ) 
                author = Person.from_dict(author_dict)
            elif role == 'coAuthor':
                fullname = author.get('gmd:CI_ResponsibleParty', {}).get('gmd:individualName', {}).get('gco:CharacterString')
                if fullname:
                    first_name = fullname.split(', ')[0]
                    last_name = fullname.split(', ')[1]
                else:
                    raise ValueError("No information about author first and last name provided.")

                organisation_name = author.get('gmd:CI_ResponsibleParty', {}).get('gmd:organisationName', {}).get('gco:CharacterString')

                coauthor_dict = dict(
                    first_name = first_name,
                    last_name = last_name,
                    organisation_name = organisation_name,
                    role = role
                    #order=i -> not sure if we need an order
                ) 
                coauthors_list.append(coauthor_dict)

        # locationShape


        # location (currently only POINT shape implemented)
        west = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:extent', {}).get('gmd:EX_Extent', {}).get('gmd:geographicElement', {}).get('gmd:EX_GeographicBoundingBox', {}).get('gmd:westBoundLongitude', {}).get('gco:Decimal')
        east = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:extent', {}).get('gmd:EX_Extent', {}).get('gmd:geographicElement', {}).get('gmd:EX_GeographicBoundingBox', {}).get('gmd:eastBoundLongitude', {}).get('gco:Decimal')
        south = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:extent', {}).get('gmd:EX_Extent', {}).get('gmd:geographicElement', {}).get('gmd:EX_GeographicBoundingBox', {}).get('gmd:southBoundLatitude', {}).get('gco:Decimal')
        north = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:extent', {}).get('gmd:EX_Extent', {}).get('gmd:geographicElement', {}).get('gmd:EX_GeographicBoundingBox', {}).get('gmd:gmd:northBoundLatitude', {}).get('gco:Decimal')

        if west == east and south == north:
            location = (west, south)

        # variable


        # embargo


        # embargo_end


        # version
        version = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:citation', {}).get('gmd:CI_Citation', {}).get('edition', {}).get('gco:CharacterString')

        # isPartial


        # publication
        publication = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:citation', {}).get('gmd:CI_Citation', {}).get('gmd:date', {}).get('gmd:CI_Date', {}).get('gmd:date', {}).get('gco:DateTime')
        # publication date also in <editionDate> 

        # lastUpdate
        last_update = metadata.get('gmd:dateStamp', {}).get('gco:DateTime')

        # keywords
        

        # license
        license_short_title = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:resourceConstraints', {}).get('gmd:MD_Constraints', {}).get('gmd:useLimitation', {}).get('gco:CharacterString')
        # license url would also be available from template
        license = api.find_license(session, short_title=license_short_title)
        
        # details


        # datasource


        # abstract
        abstract = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:abstract', {}).get('gco:CharacterString')
        
        # external_id
        
        
        # comment
         
        
        # citation'


        # associated groups
        associated_groups = metadata.get('gmd:identificationInfo', {}).get('gmd:MD_DataIdentification', {}).get('gmd:aggregationInfo', [])
        projects_labels = [] # need to find a way to differentiate projects from labels and composites from splits
        composites_splits = []
        for group in associated_groups:
            # project, label: <gmd:associationType == 'crossReference' -> not possible to identify EntryGroupType (project OR label!)
            if group.get('gmd:MD_AggregateInformation', {}).get('gmd:associationType', {}).get('gmd:DS_AssociationTypeCode', {}).get('#text') == 'largerWorkCitation':
                group_uuid = group.get('gmd:MD_AggregateInformation', {}).get('gmd:aggregateDataSetIdentifier', {}).get('gmd:RS_Identifier', {}).get('gmd:code', {}).get('gco:CharacterString')
                projects_labels.append(group_uuid)
            # composite, split dataset: <gmd:associationType> == 'largerWorkCitation' -> see above
            elif group.get('gmd:MD_AggregateInformation', {}).get('gmd:associationType', {}).get('gmd:DS_AssociationTypeCode', {}).get('#text') == 'crossReference':
                group_uuid = group.get('gmd:MD_AggregateInformation', {}).get('gmd:aggregateDataSetIdentifier', {}).get('gmd:RS_Identifier', {}).get('gmd:code', {}).get('gco:CharacterString')
                composites_splits.append(group_uuid)


        # ADD ENTRY (by hand), we could also use Entry.from_dict()

        # add the entry
        entry = api.add_entry(
            session=session,
            title=title,
            author=author,
            location=location,
            variable=variable,
            abstract=abstract,
            external_id=external_id,
            geom=geom,
            license=license,
            embargo=embargo
        )


        d = dict(
            #id=id,
            uuid=uuid,
            title=title,
            author=author_dict(deep=False),
            authors=[a.to_dict(deep=False) for a in self.authors],
            locationShape=location_shape.wkt,
            location=location_shape.wkt,
            variable=variable.to_dict(deep=False),
            embargo=embargo,
            embargo_end=embargo_end,
            version=version,
            isPartial=is_partial,
            publication=publication,
            lastUpdate=lastUpdate,
            keywords=self.plain_keywords_dict()
            # datasource
        )


        # lazy loading
        if deep:
            projects = self.projects
            if len(projects) > 0:
                d['projects'] = [p.to_dict(deep=False) for p in projects]
            comp = self.composite_entries
            if len(comp) > 0:
                d['composite_entries'] = [e.to_dict(deep=False) for e in comp]
        # HIER FEHLEN AUCH LABELS UND SPLIT DATASETS!


        coauthors = [Person.from_dict(a, session) for a in coauthors_list]
        api.add_persons_to_entries(
            session,
            entries=[entry],
            persons=coauthors,
            roles=['coAuthor'] * len(coauthors),
            order=[_ + 2 for _ in range(len(coauthors))]
        )


        # USE Entry.from_dict() after the dictionary is built??
        if commit:
            Entry.from_dict(d)
        else:
            return d
    