[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "worldbank_gdp_raw_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "indicator.id", DestinationColumnName = "indicator.id"], [SourceColumnName = "indicator.value", DestinationColumnName = "indicator.value"], [SourceColumnName = "country.id", DestinationColumnName = "country.id"], [SourceColumnName = "country.value", DestinationColumnName = "country.value"], [SourceColumnName = "countryiso3code", DestinationColumnName = "countryiso3code"], [SourceColumnName = "date", DestinationColumnName = "date"], [SourceColumnName = "value", DestinationColumnName = "value"], [SourceColumnName = "unit", DestinationColumnName = "unit"], [SourceColumnName = "obs_status", DestinationColumnName = "obs_status"], [SourceColumnName = "decimal", DestinationColumnName = "decimal"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared worldbank_gdp_raw = let
  Source = Json.Document(Web.Contents("https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json&per_page=100")),
  Navigation = Source{1},
  #"Converted to table" = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table", "Column1", {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}, {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}),
  #"Expanded indicator" = Table.ExpandRecordColumn(#"Expanded Column1", "indicator", {"id", "value"}, {"indicator.id", "indicator.value"}),
  #"Expanded country" = Table.ExpandRecordColumn(#"Expanded indicator", "country", {"id", "value"}, {"country.id", "country.value"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded country", {{"value", type number}, {"decimal", Int64.Type}, {"indicator.id", type text}, {"indicator.value", type text}, {"country.id", type text}, {"country.value", type text}, {"countryiso3code", type text}, {"date", type text}, {"unit", type text}, {"obs_status", type text}})
in
  #"Changed column type";
shared worldbank_gdp_raw_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "cda7e592-8a7b-455c-bc76-940fae1bdc29"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "4846a72e-4ce4-4b7b-9945-827654653066"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "worldbank_gdp_raw", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
