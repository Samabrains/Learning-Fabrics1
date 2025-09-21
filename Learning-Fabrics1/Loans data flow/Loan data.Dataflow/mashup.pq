[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Group_Active_Loans csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Group_Active_Loans csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "c80302c8-0ac6-4eef-988c-fae517ab7b89"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "3a113418-76d4-49cb-831f-9b58a90146ba"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "RawADLS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Group_Active_Loans.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 20, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Removed blank rows" = Table.SelectRows(#"Imported CSV", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed blank rows", {{"Column1", type text}, {"Column2", type text}, {"Column3", type text}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column7", type text}, {"Column8", type text}, {"Column9", type text}, {"Column10", type text}, {"Column11", type text}, {"Column12", type text}, {"Column13", type text}, {"Column14", type number}, {"Column15", type text}, {"Column16", type text}, {"Column17", type text}, {"Column18", type text}, {"Column19", type text}, {"Column20", type text}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"Column3", "Column14", "Column19", "Column15"}),
  #"Removed top rows" = Table.Skip(#"Removed columns", 4),
  #"Promoted headers" = Table.PromoteHeaders(#"Removed top rows", [PromoteAllScalars = true]),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Promoted headers", {{"  """, Int64.Type}, {"Phone", Int64.Type}, {"Loan Series", Int64.Type}, {"Interest", type number}, {"Principal Outstanding", type number}, {"Interest Outstanding", type number}, {"Total P + I Outstanding", type number}}),
  #"Removed bottom rows" = Table.RemoveLastN(#"Changed column type 1", 5),
  #"Removed columns 1" = Table.RemoveColumns(#"Removed bottom rows", {"_1", "  """, "Gender", "Credit Officer"}),
  #"Removed top rows 1" = Table.Skip(#"Removed columns 1", 3),
  #"Filtered rows" = Table.SelectRows(#"Removed top rows 1", each ([#""] = "Bugiri") and ([Supervisor] <> "")),
  #"Renamed columns" = Table.RenameColumns(#"Filtered rows", {{"", "Branch"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns", {"Account Names", "Customer Number", "Phone", "Loan Series"}),
  #"Filtered rows 1" = Table.SelectRows(#"Removed columns 2", each ([Loan Amount] = "Agricultural Loan Other Value Chain –ACU" or [Loan Amount] = "Agricultural Loan Other Value Chain –ACU-FIA" or [Loan Amount] = "Agricultural Loans")),
  #"Renamed columns 1" = Table.RenameColumns(#"Filtered rows 1", {{"Loan Amount", "Product"}, {"Interest", "Loan Amount"}, {"Principal Outstanding", "Interest"}, {"Interest Outstanding", "OSP"}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Renamed columns 1", {{"Interest", Int64.Type}}),
  #"Removed columns 3" = Table.RemoveColumns(#"Changed column type 2", {"Product "}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 3", {{"Total P + I Outstanding", "OSI"}, {"Supervisor", "Relationship Officer"}})
in
  #"Renamed columns 2";
shared #"Group_Active_Loans csv_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "c80302c8-0ac6-4eef-988c-fae517ab7b89"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "9365ef1c-eb8a-42d5-a0c6-d0e2f5beaab6"]}[Data],
  TableNavigation = Navigation_2{[Id = "Group_Active_Loans csv", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
