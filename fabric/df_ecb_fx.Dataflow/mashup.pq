[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "ecb_fx_raw_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared ecb_fx_raw = let
  Source = Csv.Document(Web.Contents("https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata&startPeriod=2026-01-01"), [Delimiter = ",", Columns = 32, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers 1" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers 1", {{"OBS_VALUE", type number}, {"KEY", type text}, {"FREQ", type text}, {"CURRENCY", type text}, {"CURRENCY_DENOM", type text}, {"EXR_TYPE", type text}, {"EXR_SUFFIX", type text}, {"TIME_PERIOD", type date}, {"OBS_STATUS", type text}, {"OBS_CONF", type text}, {"OBS_PRE_BREAK", type text}, {"OBS_COM", type text}, {"TIME_FORMAT", type duration}, {"BREAKS", type text}, {"COLLECTION", type text}, {"COMPILING_ORG", type text}, {"DISS_ORG", type text}, {"DOM_SER_IDS", type text}, {"PUBL_ECB", type text}, {"PUBL_MU", type text}, {"PUBL_PUBLIC", type text}, {"UNIT_INDEX_BASE", type text}, {"COMPILATION", type text}, {"COVERAGE", type text}, {"DECIMALS", Int64.Type}, {"NAT_TITLE", type text}, {"SOURCE_AGENCY", type text}, {"SOURCE_PUB", type text}, {"TITLE", type text}, {"TITLE_COMPL", type text}, {"UNIT", type text}, {"UNIT_MULT", Int64.Type}})
in
  #"Changed column type";
shared ecb_fx_raw_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "cda7e592-8a7b-455c-bc76-940fae1bdc29"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "4846a72e-4ce4-4b7b-9945-827654653066"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "ecb_fx_raw", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
