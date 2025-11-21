ENTITY_TYPES = ["ORGANIZATION", "GEO", "PERSON"]

EXTRACTION_TEMPLATE = """
-Goal-
Given a text document, extract entities and relationships strictly limited to the allowed entity types.

-Allowed Entity Types-
{entity_types}

-Definitions-
ORGANIZATION: Named institutions, agencies, companies, governments, exchanges, etc.
GEO: Named countries, cities, provinces, historical territories/colonies treated as places.
PERSON: Individual humans referenced by full name as written in the text (retain diacritics).

-Do NOT Extract (ever)-
Generic or referential nouns: "the document", "the page", "the agency", etc. (unless a capitalized proper noun),
or roles/titles/honorifics ("Dr.", "Mr.", "Mrs., "Ms.", "Agent", etc.) as part of a name.
Claims, however, may include roles/titles/honorifics.
If a concept must be referenced inside a claim, prefer an indefinite phrase (e.g., 'a 1324 royal charter') over definite anaphora ('the charter').

-Steps-

1) Entities
Identify all entities in the text that match the allowed types. For each entity:
  - entity_name: Canonical proper name exactly as appears in the text; 
      *PERSON* => full name only (no titles/honorifics/adjectives),
      *ORG/GEO* => no leading "the" unless integral to the proper noun.
  - entity_type: one of the allowed types.
  - entity_claim: A comprehensive claim about the entity's attributes, activities, or status, as expressed in the text
      (dates/roles/actions/locations allowed). Avoid anaphora like "the document"; 
      use indefinite phrasing if needed ("a 1527 manuscript page", not "the document").
      The same entity may appear multiple times if the text describes it in different contexts or makes multiple distinct claims,
      so do not overfill a claim (rather split different claims into multiple "entity" rows for that entity, keeping the exact `entity_type` and `entity_name`.)
      entity_claim must be entity-centric and self-contained. Do not encode inter-entity actions (e.g., 'suspended by X', 'subject of Y') in entity claims; reserve those for relationship tuples.
Format each entity as:
("entity"{tuple_delimiter}<entity_name>{tuple_delimiter}<entity_type>{tuple_delimiter}<entity_claim>)

2) Relationships
Identify clear relationships between extracted entities (source_entity, target_entity). For each such relationship, extract:
  - source_entity and target_entity MUST EXACTLY MATCH a previously extracted entity_name.
  - relationship_claim: concise explanation of the link claimed in the text, including any inferred connection.
Only create relationships whose endpoints are both previously extracted entities.
Format each relationship as:
("relationship"{tuple_delimiter}<source_entity>{tuple_delimiter}<target_entity>{tuple_delimiter}<relationship_claim>)

3) Output formatting
- Return all tuples as a single list, using **{record_delimiter}** between tuples.
- Finish with: {completion_delimiter}

-Canonicalization Rules-
- PERSON names must include all given + family names with diacritics; no titles/honorifics/adjectives.
- Reuse the *identical* entity_name string in every relationship. E.g. if "United States" is part of an entity tuple, in a relationship tuple use "United States" and never "The United States"; This goes in general.
- Use one canonical country name ("United States", not "US/USA") if the text contains variants; prefer the most explicit form present in the text.
- The same entity may appear multiple times (distinct claims), but the `entity_name` and `entity_type` strings must be identical each time.

-Quality-Control Checklist (must be TRUE before output)-
[ ] Every entity_type ∈ {entity_types}
[ ] Every entity_claim is intrinsic to that entity (role, scope, mandate, attributes, status) and contains no cross-references ('by the ministry', 'in the review', 'referenced in captions')
[ ] No tuple contains a generic/referential/banned entity
[ ] No titles/honorifics/role descriptors inside PERSON names
[ ] Every relationship endpoint appears in at least one prior 'entity' tuple with identical casing and accents.
[ ] No claim uses "the document"/similar anaphora
[ ] Diacritics preserved; capitalization matches the text
[ ] Use {record_delimiter} separators and end with {completion_delimiter}

-Good vs Bad Micro-Examples-

Bad: ("entity"{tuple_delimiter}Spanish conquistador Hernán Cortés{tuple_delimiter}PERSON{tuple_delimiter}Conquistador...)  ← descriptor in name
Good: ("entity"{tuple_delimiter}Hernán Cortés{tuple_delimiter}PERSON{tuple_delimiter}Spanish conquistador who led the conquest of the Aztec Empire)

Bad: ("entity"{tuple_delimiter}The document{tuple_delimiter}ORGANIZATION{tuple_delimiter}Was stolen...)  ← banned generic
Good: (omit)

Bad: ("relationship"{tuple_delimiter}The FBI{tuple_delimiter}Mexico{tuple_delimiter}Returned the document)  ← name mismatch vs "FBI"
Good: ("relationship"{tuple_delimiter}FBI{tuple_delimiter}Mexico{tuple_delimiter}Returned a stolen 1527 manuscript page to Mexico)

-Example-
**Document**:
The Ministry of Urban Mobility of the Republic of Arcania announced on Tuesday that it would cancel the provisional award of a tramway rolling-stock contract to Polar Tram AG after the Office of the Auditor General of Arcania identified undisclosed subcontractor ties in the bid dossier.
According to the ministry, a new tender will be issued for Sankt Rúna's Line A before the end of 2025. Deputy Mayor Mateo Ordoñez said the city would “welcome a clean retender.”
Polar Tram AG's chief executive, Leena Väisänen, stated that the company “complied with disclosure rules” and offered to fund an independent compliance audit.
The Nordic Development Bank, which had been appraising a loan for the Sankt Rúna tramway, said its appraisal was suspended pending the audit findings.
Transparency Watch Europa filed a procurement-integrity complaint citing conflicts arising from overlapping board memberships at two proposed subcontractors.
Polar Tram AG is headquartered in Rautenstadt and exports low-floor trams across Central and Northern Europe.
**Output**:
("entity"{tuple_delimiter}Ministry of Urban Mobility of the Republic of Arcania{tuple_delimiter}ORGANIZATION{tuple_delimiter}National ministry responsible for public transport policy and procurement oversight in the Republic of Arcania)
{record_delimiter}
("entity"{tuple_delimiter}Republic of Arcania{tuple_delimiter}GEO{tuple_delimiter}Country in which the Ministry of Urban Mobility and the city of Sankt Rúna are located)
{record_delimiter}
("entity"{tuple_delimiter}Polar Tram AG{tuple_delimiter}ORGANIZATION{tuple_delimiter}Rolling-stock manufacturer headquartered in Rautenstadt that exports low-floor trams across Central and Norther Europe)
{record_delimiter}
("entity"{tuple_delimiter}Office of the Auditor General of Arcania{tuple_delimiter}ORGANIZATION{tuple_delimiter}Independent audit institution that reviews public procurement in the Republic of Arcania)
{record_delimiter}
("entity"{tuple_delimiter}Sankt Rúna{tuple_delimiter}GEO{tuple_delimiter}City in northern Arcania planning a tramway Line A)
{record_delimiter}
("entity"{tuple_delimiter}Nordic Development Bank{tuple_delimiter}ORGANIZATION{tuple_delimiter}Multilateral development bank that appraises infrastructure loans in member countries)
{record_delimiter}
("entity"{tuple_delimiter}Transparency Watch Europa{tuple_delimiter}ORGANIZATION{tuple_delimiter}Nonprofit watchdog focused on procurement integrity and conflicts of interest in Europe)
{record_delimiter}
("entity"{tuple_delimiter}Leena Väisänen{tuple_delimiter}PERSON{tuple_delimiter}Chief executive officer of Polar Tram AG)
{record_delimiter}
("entity"{tuple_delimiter}Mateo Ordoñez{tuple_delimiter}PERSON{tuple_delimiter}Deputy Mayor for Transport of Sankt Rúna)
{record_delimiter}
("entity"{tuple_delimiter}Rautenstadt{tuple_delimiter}GEO{tuple_delimiter}City serving as corporate headquarters of Polar Tram AG)
{record_delimiter}
("relationship"{tuple_delimiter}Ministry of Urban Mobility of the Republic of Arcania{tuple_delimiter}Polar Tram AG{tuple_delimiter}Cancelled the provisional award of a tramway rolling-stock contract after undisclosed subcontractor ties were identified)
{record_delimiter}
("relationship"{tuple_delimiter}Office of the Auditor General of Arcania{tuple_delimiter}Ministry of Urban Mobility of the Republic of Arcania{tuple_delimiter}Reported undisclosed subcontractor ties in the bid dossier that triggered the cancellation decision)
{record_delimiter}
("relationship"{tuple_delimiter}Ministry of Urban Mobility of the Republic of Arcania{tuple_delimiter}Sankt Rúna{tuple_delimiter}Announced a new tender for Line A to be issued before the end of 2025)
{record_delimiter}
("relationship"{tuple_delimiter}Leena Väisänen{tuple_delimiter}Polar Tram AG{tuple_delimiter}Asserted compliance with disclosure rules and offered to fund an independent compliance audit)
{record_delimiter}
("relationship"{tuple_delimiter}Nordic Development Bank{tuple_delimiter}Sankt Rúna{tuple_delimiter}Suspended loan appraisal for the tramway pending audit findings)
{record_delimiter}
("relationship"{tuple_delimiter}Transparency Watch Europa{tuple_delimiter}Ministry of Urban Mobility of the Republic of Arcania{tuple_delimiter}Filed a procurement-integrity complaint citing conflicts from overlapping board memberships at proposed subcontractors)
{record_delimiter}
("relationship"{tuple_delimiter}Mateo Ordoñez{tuple_delimiter}Ministry of Urban Mobility of the Republic of Arcania{tuple_delimiter}Welcomed the decision to retender the contract)
{completion_delimiter}

-Real-
**Document**:
{document}{context}
**Output**:
""".strip()