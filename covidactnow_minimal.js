(function() {
    // Create the connector object
    var myConnector = tableau.makeConnector();

    // Define the schema
    myConnector.getSchema = function(schemaCallback) {
        var cols = [
        {
            id: "county",
            dataType: tableau.dataTypeEnum.string,
            geoRole: tableau.geographicRoleEnum.county
        } 
        ]; 



        // tables 
        var tableSchema = {
            id: "covidActNow",
            alias: "covidActNow single value data from CA",
            columns: cols
        };


        schemaCallback([tableSchema]);
    };

    // Download the data
    myConnector.getData = function(table, doneCallback) {
        $.getJSON("https://api.covidactnow.org/v2/county/CA.timeseries.json?apiKey=c8089a2218c142a2b7e9083d55d29ca5", function(resp) {
            var tableData = [];   // store the result here

            // per row in the output array (one row per fips)
            for (var j = 0, len = resp.length; j < len; j++) {

                var  thisrow = resp[j]

                // Single valued objects per row 
                if (table.tableInfo.id == "covidActNow") {
                        // single-valued fields in JSON 
                        tableData.push({
                            "county" = thisrow.county
                        });
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
