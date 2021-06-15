(function() {
    // Create the connector object
    var myConnector = tableau.makeConnector();

    // Define the schema
    myConnector.getSchema = function(schemaCallback) {
        var cols = [{
            id: "country",
            dataType: tableau.dataTypeEnum.string,
            geoRole: tableau.geographicRoleEnum.country_region
        }, {
            id: "state",
            dataType: tableau.dataTypeEnum.string,
            geoRole: tableau.geographicRoleEnum.state_province
        }, {
            id: "county",
            dataType: tableau.dataTypeEnum.string,
            geoRole: tableau.geographicRoleEnum.county
        }, {
            id: "fips",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "lat",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "long",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "locationId",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "population",
            dataType: tableau.dataTypeEnum.integer
        }, {
            id: "lastUpdatedDate",
            dataType: tableau.dataTypeEnum.date        }
        ]; 

        var colsats = [
        {
            id: "fips",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "date",
            dataType: tableau.dataTypeEnum.date
        }, {
            id: "actuals.cases",
            dataType: tableau.dataTypeEnum.integer
        }, {
            id: "actuals.deaths",
            dataType: tableau.dataTypeEnum.integer
        }, {
            id: "actuals.positiveTests",
            dataType: tableau.dataTypeEnum.integer
        }, {
            id: "actuals.negativeTests",
            dataType: tableau.dataTypeEnum.integer
        }, {
            id: "actuals.contactTracers",
            dataType: tableau.dataTypeEnum.integer
        }, {
            id: "actuals.hospitalBeds.capacity",
            dataType: tableau.dataTypeEnum.float
        }, {
            id: "actuals.hospitalBeds.currentUsageTotal",
            dataType: tableau.dataTypeEnum.float
        }, {
            id: "actuals.hospitalBeds.currentUsageCovid",
            dataType: tableau.dataTypeEnum.float
        }, {
            id: "actuals.hospitalBeds.typicalUsageRate",
            dataType: tableau.dataTypeEnum.float
        }, {
            id: "actuals.icuBeds.capacity",
            dataType: tableau.dataTypeEnum.float
        }, {
            id: "actuals.icuBeds.currentUsageTotal",
            dataType: tableau.dataTypeEnum.float
        }, {
            id: "actuals.icuBeds.currentUsageCovid",
            dataType: tableau.dataTypeEnum.float
        }, {
            id: "actuals.icuBeds.typicalUsageRate",
            dataType: tableau.dataTypeEnum.float
        }, {
            id: "actuals.newCases",
            dataType: tableau.dataTypeEnum.integer
        }, {
            id: "actuals.vaccinesDistributed",
            dataType: tableau.dataTypeEnum.integer
        }, {
            id: "actuals.vaccinationsInitiated",
            dataType: tableau.dataTypeEnum.integer
        }, {
            id: "actuals.vaccinationsCompleted",
            dataType: tableau.dataTypeEnum.integer
        }, {
            id: "actuals.newDeaths",
            dataType: tableau.dataTypeEnum.integer
        }, {
            id: "actuals.vaccinesAdministered",
            dataType: tableau.dataTypeEnum.integer
        }        
        ];

        var colsmts = [
        {
            id: "fips",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "date",
            dataType: tableau.dataTypeEnum.date
        }, {
            id: "metrics.testPositivityRatio",
            dataType: tableau.dataTypeEnum.float
        //}, {
        //    id: "metrics.testPositivityRatioDetails",
        //    dataType: tableau.dataTypeEnum.string
        }, {
            id: "metrics.caseDensity",
            dataType: tableau.dataTypeEnum.float
        }, {
            id: "metrics.contactTracerCapacityRatio",
            dataType: tableau.dataTypeEnum.float
        }, {
            id: "metrics.infectionRate",
            dataType: tableau.dataTypeEnum.float
        }, {
            id: "metrics.infectionRateCI90",
            dataType: tableau.dataTypeEnum.float
        }, {
            id: "metrics.icuHeadroomRatio",
            dataType: tableau.dataTypeEnum.float
        //}, {
        //    id: "metrics.icuHeadroomDetails",
        //    dataType: tableau.dataTypeEnum.string
        }, {
            id: "metrics.icuCapacityRatio",
            dataType: tableau.dataTypeEnum.float
        }, {
            id: "metrics.vaccinationsInitiatedRatio",
            dataType: tableau.dataTypeEnum.float
        }, {
            id: "metrics.vaccinationsCompletedRatio",
            dataType: tableau.dataTypeEnum.float
        }
        ];


        var colsrts = [
        {
            id: "fips",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "date",
            dataType: tableau.dataTypeEnum.date
        }, {            
            id: "riskLevels.overall",
            dataType: tableau.dataTypeEnum.float
        }
        ]


        // tables 

        var tableSchemaATS = {
            id: "covidActNowATS",
            alias: "covidActNow actuals time series data from CA",
            columns: colsats
        };

        var tableSchemaMTS = {
            id: "covidActNowMTS",
            alias: "covidActNow metrics time series data from CA",
            columns: colsmts
        };

        var tableSchemaRTS = {
            id: "covidActNowRTS",
            alias: "covidActNow risk level time series data from CA",
            columns: colsrts
        };

        var tableSchema = {
            id: "covidActNow",
            alias: "covidActNow single value data from CA",
            columns: cols
        };


        schemaCallback([tableSchema, tableSchemaRTS, tableSchemaATS, tableSchemaMTS]);
    };

    // Download the data
    myConnector.getData = function(table, doneCallback) {
        $.getJSON("https://api.covidactnow.org/v2/county/CA.timeseries.json?apiKey=c8089a2218c142a2b7e9083d55d29ca5", function(resp) {
            var tableData = [];   // store the result here

            // per row in the output array (one row per fips)
            for (var j = 0, len = resp.length; j < len; j++) {

                var  thisrow = resp[j],
                    metricsts = thisrow.metricsTimeseries,
                    actualsts = thisrow.actualsTimeseries,
                    riskts = thisrow.riskLevelsTimeseries


                // Single valued objects per row 
                if (table.tableInfo.id == "covidActNow") {
                        // single-valued fields in JSON 
                        tableData.push({
                            "fips" = thisrow.fips,
                            "country" = thisrow.country,
                            "state" = thisrow.state,
                            "county" = thisrow.county,
                            "lat" = thisrow.lat,
                            "locationId" = thisrow.locationId,
                            "long" = thisrow.long,
                            "population" = thisrow.population,
                            "lastUpdatedDate" = thisrow.lastUpdatedDate
                        });
                }

                // Actuals time series objects
                // Iterate over the JSON object
                if (table.tableInfo.id == "covidActNowATS") {
                    for (var i = 0, len = actualsts.length; i < len; i++) {
                        tableData.push({
                            "date": actualsts[i].date,
                            "fips" = thisrow.fips,

                            "actuals.cases": actualsts[i].cases,
                            "actuals.deaths": actualsts[i].deaths,
                            "actuals.positiveTests": actualsts[i].positiveTests,
                            "actuals.negativeTests": actualsts[i].negativeTests,
                            "actuals.contactTracers": actualsts[i].contactTracers,
                            "actuals.hospitalBeds.capacity": actualsts[i].hospitalBeds.capacity,
                            "actuals.hospitalBeds.currentUsageTotal": actualsts[i].hospitalBeds.currentUsageTotal,
                            "actuals.hospitalBeds.currentUsageCovid": actualsts[i].hospitalBeds.currentUsageCovid,
                            "actuals.hospitalBeds.typicalUsageRate": actualsts[i].hospitalBeds.typicalUsageRate,
                            "actuals.icuBeds.capacity": actualsts[i].icuBeds.capacity,
                            "actuals.icuBeds.currentUsageTotal": actualsts[i].icuBeds.currentUsageTotal, 
                            "actuals.icuBeds.currentUsageCovid": actualsts[i].icuBeds.currentUsageCovid, 
                            "actuals.icuBeds.typicalUsageRate": actualsts[i].icuBeds.typicalUsageRate, 
                            "actuals.newCases": actualsts[i].newCases, 
                            "actuals.vaccinesDistributed": actualsts[i].vaccinesDistributed, 
                            "actuals.vaccinationsInitiated": actualsts[i].vaccinationsInitiated, 
                            "actuals.vaccinationsCompleted": actualsts[i].vaccinationsCompleted, 
                            "actuals.newDeaths": actualsts[i].newDeaths, 
                            "actuals.vaccinesAdministered": actualsts[i].vaccinesAdministered
                        });
                    } // for i loop end
                }                 

                // Metrics time series objects
                // Iterate over the JSON object
                if (table.tableInfo.id == "covidActNowMTS") {
                    for (var i = 0, len = metricsts.length; i < len; i++) {
                        tableData.push({
                            "date": metricsts[i].date,
                            "fips" = thisrow.fips,

                            "metrics.testPositivityRatio": metricsts[i].testPositivityRatio, 
                            // "metrics.testPositivityRatioDetails": metricsts[i].testPositivityRatioDetails, 
                            "metrics.caseDensity": metricsts[i].caseDensity, 
                            "metrics.contactTracerCapacityRatio": metricsts[i].contactTracerCapacityRatio,
                            "metrics.infectionRate": metricsts[i].infectionRate,
                            "metrics.infectionRateCI90": metricsts[i].infectionRateCI90,
                            "metrics.icuHeadroomRatio": metricsts[i].icuHeadroomRatio,
                            // "metrics.icuHeadroomDetails": metricsts[i].icuHeadroomDetails,
                            "metrics.icuCapacityRatio": metricsts[i].icuCapacityRatio,
                            "metrics.vaccinationsInitiatedRatio": metricsts[i].vaccinationsInitiatedRatio,
                            "metrics.vaccinationsCompletedRatio": metricsts[i].vaccinationsCompletedRatio,

                        });
                    } // for i loop end
                }

                // risk level time series objects
                // Iterate over the JSON object
                if (table.tableInfo.id == "covidActNowRTS") {
                    for (var i = 0, len = riskts.length; i < len; i++) {
                        tableData.push({
                            "date": riskts[i].date,
                            "fips" = thisrow.fips,

                            "riskLevels.overall": riskts[i].overall,

                        });
                    } // for i loop end
                }



            } // for j loop end

            table.appendRows(tableData);
            doneCallback();
        }); // end .getJSON
    };

    tableau.registerConnector(myConnector);

    // Create event listeners for when the user submits the form
    $(document).ready(function() {
        $("#submitButton").click(function() {
            tableau.connectionName = "COVID Act Now CA Timeseries"; // This will be the data source name in Tableau
            tableau.submit(); // This sends the connector object to Tableau
        });
    });
})();
