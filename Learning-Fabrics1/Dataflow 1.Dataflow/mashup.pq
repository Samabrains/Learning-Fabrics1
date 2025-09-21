[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Fabric_TutorialAdventureWorks_Product_Categories_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Fabric_TutorialAdventureWorks_Product_Categories = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "c80302c8-0ac6-4eef-988c-fae517ab7b89"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "3a113418-76d4-49cb-831f-9b58a90146ba"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "Fabric_TutorialAdventureWorks_Product_Categories"]}[Content],
  Navigation = #"Navigation 4"{[Name = "AdventureWorks_Product_Categories.csv"]}[Content],
  #"Imported CSV" = Csv.Document(Navigation, [Delimiter = ",", Columns = 2, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"ProductCategoryKey", Int64.Type}, {"CategoryName", type text}}),
  #"Uppercased text" = Table.TransformColumns(#"Changed column type", {{"CategoryName", each Text.Upper(_), type nullable text}}),
  #"Reordered columns" = Table.ReorderColumns(#"Uppercased text", {"CategoryName", "ProductCategoryKey"})
in
  #"Reordered columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Fabric_TutorialProduct_Subcategories_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Fabric_TutorialProduct_Subcategories = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "c80302c8-0ac6-4eef-988c-fae517ab7b89"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "3a113418-76d4-49cb-831f-9b58a90146ba"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "Fabric_TutorialProduct_Subcategories"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "AdventureWorks_Product_Subcategories.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 3, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"ProductSubcategoryKey", Int64.Type}, {"SubcategoryName", type text}, {"ProductCategoryKey", Int64.Type}}),
  #"Uppercased text" = Table.TransformColumns(#"Changed column type", {{"SubcategoryName", each Text.Upper(_), type nullable text}})
in
  #"Uppercased text";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Merge_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Merge = let
  Source = Table.NestedJoin(Fabric_TutorialProduct_Subcategories, {"ProductCategoryKey"}, Fabric_TutorialAdventureWorks_Product_Categories, {"ProductCategoryKey"}, "Fabric_TutorialAdventureWorks_Product_Categories", JoinKind.LeftOuter),
  #"Expanded Fabric_TutorialAdventureWorks_Product_Categories" = Table.ExpandTableColumn(Source, "Fabric_TutorialAdventureWorks_Product_Categories", {"ProductCategoryKey", "CategoryName"}, {"ProductCategoryKey.1", "CategoryName"})
in
  #"Expanded Fabric_TutorialAdventureWorks_Product_Categories";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Group_Active_Loans csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Group_Active_Loans = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "c80302c8-0ac6-4eef-988c-fae517ab7b89"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "3a113418-76d4-49cb-831f-9b58a90146ba"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "RawADLS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Group_Active_Loans.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 20, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Removed blank rows" = Table.SelectRows(#"Imported CSV", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed blank rows", {{"Column1", type text}, {"Column2", type text}, {"Column3", type text}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column7", type text}, {"Column8", type text}, {"Column9", type text}, {"Column10", type text}, {"Column11", type text}, {"Column12", type text}, {"Column13", type text}, {"Column14", type number}, {"Column15", type text}, {"Column16", type text}, {"Column17", type text}, {"Column18", type text}, {"Column19", type text}, {"Column20", type text}}),
  #"Removed top rows" = Table.Skip(#"Changed column type", 4),
  #"Promoted headers" = Table.PromoteHeaders(#"Removed top rows", [PromoteAllScalars = true]),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Promoted headers", {{"  """, Int64.Type}, {"Account", Int64.Type}, {"Phone", Int64.Type}, {"Loan Series", Int64.Type}, {"Interest", type number}, {"Principal Outstanding", type number}, {"Interest Outstanding", type number}, {"Total P + I Outstanding", type number}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type 1", {{"Account Names", "Account Number"}, {"Customer Number", "Customer Name"}, {"Phone", "Customer Number"}, {"Loan Series", "Phone"}, {"Product ", "Loan Series"}, {"Loan Amount", "Product"}, {"Interest", "Loan Amount"}, {"Principal Outstanding", "Interest"}, {"Interest Outstanding", "Principal Outstanding"}, {"Total P + I Outstanding", "Interest Outstanding"}, {"Column14", "Total P + I Outstanding"}, {"_1", "Loan_terms"}, {"Supervisor", "Relationship officer"}, {"Gender", "Supervisor"}, {"_2", "Gender"}, {"_3", "Group Customer Number"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"  """, "Account"}),
  #"Removed top rows 1" = Table.Skip(#"Removed columns", 3),
  #"Removed columns 1" = Table.RemoveColumns(#"Removed top rows 1", {"Group Customer Number"}),
  #"Removed blank rows 1" = Table.SelectRows(#"Removed columns 1", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
  #"Removed columns 2" = Table.RemoveColumns(#"Removed blank rows 1", {"Credit Officer"}),
  #"Removed bottom rows" = Table.RemoveLastN(#"Removed columns 2", 5),
  #"Removed duplicates" = Table.Distinct(#"Removed bottom rows", {"Interest Outstanding"}),
  #"Removed columns 3" = Table.RemoveColumns(#"Removed duplicates", {"Total P + I Outstanding", "Supervisor"}),
  #"Filtered rows" = Table.SelectRows(#"Removed columns 3", each ([#""] = "Bugiri")),
  #"Removed columns 4" = Table.RemoveColumns(#"Filtered rows", {"Gender", "Loan_terms"})
in
  #"Removed columns 4";
shared Merge_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "c80302c8-0ac6-4eef-988c-fae517ab7b89"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "9365ef1c-eb8a-42d5-a0c6-d0e2f5beaab6"]}[Data],
  TableNavigation = Navigation_2{[Id = "Consolidated", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"Group_Active_Loans csv_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "c80302c8-0ac6-4eef-988c-fae517ab7b89"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "9365ef1c-eb8a-42d5-a0c6-d0e2f5beaab6"]}[Data],
  TableNavigation = Navigation_2{[Id = "Loans", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared Fabric_TutorialProduct_Subcategories_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "c80302c8-0ac6-4eef-988c-fae517ab7b89"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "9365ef1c-eb8a-42d5-a0c6-d0e2f5beaab6"]}[Data],
  TableNavigation = Navigation_2{[Id = "Fabric_TutorialProduct_Subcategories", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared Fabric_TutorialAdventureWorks_Product_Categories_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "c80302c8-0ac6-4eef-988c-fae517ab7b89"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "9365ef1c-eb8a-42d5-a0c6-d0e2f5beaab6"]}[Data],
  TableNavigation = Navigation_2{[Id = "Fabric_TutorialAdventureWorks_Product_Categories", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
