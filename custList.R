#
# This program generates a report of customers and quantities by customer for a list of part numbers.
# The idea is that when someone buys a mold, we use this to give them
# a list of sales from the mold, given the part numbers from the mold.
#
# The report format is...
#   ByItemSection
#   CustomerSection
#
# The ByItemSection has one section for each item to report on
#   Each section is a sorted (descending by quantity) list of customers and quantity purchased
#
# The CustomerSection gives contact information for each customer mentioned in the ByItemSections section.
# The customers are listed in order by customer number, which is part of the ByItemSection fields.
# Customers are not duplicated when they occur multiple times in the ByItemSection
#



require("magrittr")

querySBT <- function(x)
{
  require("RODBC")
  connection <- paste(
    "BackgroundFetch=No;",
    "DSN=Visual FoxPro Tables;",
    "UID=;",
    "SourceType=DBF;",
    "Collate=Machine;",
    "SourceDB=h:/sbt;",
    "Exclusive=No")
  channel <- odbcDriverConnect(connection=connection)
  ok<-odbcQuery(channel,"SET DELETED ON")
  stopifnot(ok==1)
  sqlQuery(channel,"select * from artran01 where c2tod(invdte)<={1/1/1983}")
  dat <-sqlQuery(channel,x,as.is=T)
  if(class(dat)!="data.frame") stop(dat)
  odbcClose(channel)
  dat
}

trimRight <- function(x,fieldName=NULL) {
	if(is.null(fieldName)) {
		x <- sub(" +$", "", x)
	} else {
		x[,fieldName] <- sub(" +$", "", x[,fieldName])
	}
	x
}
toNumeric <- function(x,fieldName=NULL) {
	if(is.null(fieldName)) {
		x <- as.numeric(x)
	} else {
		x[,fieldName] <- as.numeric(x[,fieldName])
	}
	x
}

#
# Output of this report is best viewed by running it through writeLines
#
report <- function(partNumbers)
{
	stopifnot(class(partNumbers)=="character")
	
	addCustomersAndQuantities <- function(itemVec) {
		addForOneItem <- function(item) {
			customersAndQuantities <- function(item) {
				paste0(
					"select iif(substr(custno,1,3)=='MET','MET',custno) as custno,",
					"sum(qtyshp) as qtyshp ",
					"from arytrn01 where year(ctod(invdte))==2019 ",
					"and item=='",item,"' ",
					"group by 1 order by 2 desc") %>%
				querySBT %>%
				trimRight("custno") %>%
				toNumeric("qtyshp")
			}
			list(item,customersAndQuantities(item))
		}
		lapply(itemVec,addForOneItem)
	}

	addCompanyDetails <- function(byItem) {
		getDetailsFromByItem <- function() {
			getDetails <- function() {
				itemVector <- lapply(byItem,function(x)x[[1]]) %>% unlist
				itemListString <- paste0('"',paste(itemVector,collapse='","'),'"')
				paste0(
					"select custno,company,contact,address1,address2,address3,city,state,zip,country,phone ",
					"from arcust01 where custno in ",
						"(select custno from arytrn01 where item in (",itemListString,") and year(ctod(invdte))=2019) ",
					"order by 1"
				) %>%
				querySBT
			}
			getDetails()
		}
		list(byItem,getDetailsFromByItem())
	}
	
	formatReport <- function(reportData) {
		formatSales <- function(sales) {
			humanReadableDataframe <- function(df) {
				if(nrow(df)!=0) { df } else { "None." }
			}
			paste0(
				"**",sales[[1]],"**\n",
				capture.output( humanReadableDataframe(sales[[2]]) ) %>% paste(.,collapse="\n"),
				"\n"
			)
		}
		formatCustomers <- function(row) {
			cust <- reportData[[2]][row,]
			paste0("** ",cust$custno, " **\n",
				cust$company, "\n",
				cust$contact, "\n",
				cust$address1, "\n",
				if(cust$address2%>%trimRight != "") {paste0(cust$address2, "\n")} else {NULL},
				if(cust$address3%>%trimRight != "") {paste0(cust$address3, "\n")} else {NULL},
				cust$city %>% trimRight, " ", cust$state, " ", cust$zip, "\n",
				if(cust$country%>%trimRight != "") {paste0(cust$country, "\n")} else {NULL},
				cust$phone, "\n"
			)
		}
		paste(
			"Sales For Items:\n\n",
			paste(lapply(reportData[[1]],formatSales),collapse="\n"),
			"\nCustomer Information:\n\n",
			paste(lapply(1:nrow(reportData[[2]]),formatCustomers),collapse="\n"),
			collapse="\n"
		)
	}
	
	partNumbers %>% trimRight %>% addCustomersAndQuantities %>% addCompanyDetails %>% formatReport
}