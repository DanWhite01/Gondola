import React, { useEffect, useRef } from "react";
import { render } from "react-dom";
import { AgGridReact, AgGridColumn } from "ag-grid-react";
import "ag-grid-community/dist/styles/ag-grid.css";
import "ag-grid-community/dist/styles/ag-theme-alpine.css";
import { makeStyles } from "@material-ui/core/styles";
import FormGroup from "@material-ui/core/FormGroup";
import FormControlLabel from "@material-ui/core/FormControlLabel";
import Checkbox from "@material-ui/core/Checkbox";
import TextField from "@material-ui/core/TextField";
import ButtonCellRenderer from "./ButtonCellRenderer";
import DiffCellRenderer from "./DiffCellRenderer";
import { saveAs } from "file-saver";
import Button from "@material-ui/core/Button";
import MD5 from "crypto-js/md5";

const useStyles = makeStyles((theme) => ({
  root: {
    "& > *": {
      margin: theme.spacing(1),
    },
  },

  exampleWrapper: {
    display: "flex",
    flexDirection: "column",
    height: "100%",
  },

  myGrid: {
    flex: "1 1 0px",
    width: "100%",
  },
  exampleHeader: {
    fontFamily: ["Verdana", "Geneva", "Tahoma", "sansSerif"],
    fontSize: "13px",
    marginBottom: "5px",
  },
}));

const AGGridResults = (props) => {
  console.log(props);

  const classes = useStyles();

  const [statusFilter, setStatusFilter] = React.useState({
    newObjects: true,
    deletedObjects: true,
    modifiedObjects: true,
    unchangedObjects: false,
  });

  const [jiraNumber, setJiraNumber] = React.useState("");

  const [selectedCount, setSelectedCount] = React.useState(0);

  // // Similar to componentDidMount and componentDidUpdate:
  useEffect(() => {
    console.log("UseEffect 1");
    statusFilterRef.current = { ...statusFilterRef.current, ...statusFilter };
    gridApi && gridApi.onFilterChanged();
  }, [statusFilter]);

  const statusFilterRef = useRef();

  const [gridApi, setGridApi] = React.useState(null);
  const [gridColumnApi, setGridColumnApi] = React.useState(null);

  const onGridReady = (params) => {
    console.log("onGridReady");
    setGridApi(params.api);
    setGridColumnApi(params.columnApi);
  };

  const onQuickFilterChanged = () => {
    console.log("onQuickFilterChanged");
    gridApi.setQuickFilter(document.getElementById("quickFilter").value);
  };

  const handleFilterChange = (event) => {
    console.log("handleFilterChange");
    setStatusFilter({
      ...statusFilter,
      [event.target.name]: event.target.checked,
    });
    console.log(statusFilter);
  };
  function isExternalFilterPresent() {
    return true;
  }

  const handleSelectionChanged = (event) => {
    var rowCount = event.api.getSelectedNodes().length;
    setSelectedCount(rowCount);
    console.log(rowCount);
  };

  const doesExternalFilterPass = (node) => {
    switch (node.data._action) {
      // case 'NEW': return statusFilter.newObjects
      // case 'DROP': return statusFilter.deletedObjects
      // case 'MODIFY': return statusFilter.modifiedObjects
      // case 'NO CHANGE': return statusFilter.unchangedObjects

      case "NEW":
        return statusFilterRef.current.newObjects;
      case "DROP":
        return statusFilterRef.current.deletedObjects;
      case "MODIFY":
        return statusFilterRef.current.modifiedObjects;
      case "NO CHANGE":
        return statusFilterRef.current.unchangedObjects;

      default:
        return true;
    }
  };

  useEffect(() => {
    console.log("UseEffect 2");
    setStatusFilter({
      newObjects: true,
      deletedObjects: true,
      modifiedObjects: true,
      unchangedObjects: false,
    });
    setSelectedCount(0);
    if (gridApi) {
      props.dbComparison.loading
        ? gridApi.showLoadingOverlay()
        : gridApi.hideOverlay();
    }
  }, [props]);

  const generateDDL = () => {
    var zip1 = require("jszip")();
    let feature_log = [];
    let manifest_file = [];
    let full_ddl = [];

    manifest_file.push("ticket_number: " + jiraNumber);
    manifest_file.push("created_by: MGREEN");
    manifest_file.push("");
    manifest_file.push("sql_files:");
    manifest_file.push("");

    gridApi.getSelectedNodes().forEach((d) => {
      console.log("Download DDL Testing");
      console.log(d);
      //Create the Directory Structure
      zip1.file(
        d.data["schema_name"].toLowerCase() +
          "/" +
          d.data["db_object_type"].toLowerCase() +
          "/" +
          d.data["object_name"].toLowerCase() +
          ".sql",
        d.data.change_ddl
      );

      manifest_file.push("  - id: " + MD5(d.data.change_ddl).toString());
      manifest_file.push(
        "  - sql_location: " +
          d.data["schema_name"].toLowerCase() +
          "/" +
          d.data["db_object_type"].toLowerCase() +
          "/" +
          (d.data["db_object_type"].toLowerCase() === "schema"
            ? d.data["schema_name"].toLowerCase()
            : d.data["object_name"].toLowerCase()) +
          ".sql"
      );
      manifest_file.push("");
      full_ddl.push(d.data.change_ddl);
    });

    zip1.file("feature_log/manifest.yml", manifest_file.join("\n"));
    zip1.file("full_ddl.ddl", full_ddl.join("\n"));
    zip1.generateAsync({ type: "blob" }).then((content) => {
      saveAs(content, "Gondala_Upgrade_Script.zip");
    });
  };

  return (
    <div style={{ width: "100%", height: 600 }}>
      <div className={classes.exampleWrapper}>
        <div style={{ marginBottom: "5px" }}>
          {/* <input
                        type="text"
                        onInput={() => onQuickFilterChanged()}
                        id="quickFilter"
                        placeholder="quick filter..."
                    /> */}
          <FormGroup row>
            <FormControlLabel
              style={{ userSelect: "none" }}
              control={
                <Checkbox
                  style={{ color: "#57b6b2" }}
                  checked={statusFilter.newObjects}
                  onChange={handleFilterChange}
                  name="newObjects"
                />
              }
              label={`New objects in Source DB (${
                props.dbComparison.results.filter(
                  (obj) => obj._action === "NEW"
                ).length
              })`}
            />
            <FormControlLabel
              style={{ userSelect: "none" }}
              control={
                <Checkbox
                  style={{ color: "#57b6b2" }}
                  checked={statusFilter.modifiedObjects}
                  onChange={handleFilterChange}
                  name="modifiedObjects"
                />
              }
              label={`Modified Objects (${
                props.dbComparison.results.filter(
                  (obj) => obj._action === "MODIFY"
                ).length
              })`}
            />
            <FormControlLabel
              style={{ userSelect: "none" }}
              control={
                <Checkbox
                  style={{ color: "#57b6b2" }}
                  checked={statusFilter.deletedObjects}
                  onChange={handleFilterChange}
                  name="deletedObjects"
                />
              }
              label={`Deleted Objects in Source DB (${
                props.dbComparison.results.filter(
                  (obj) => obj._action === "DROP"
                ).length
              })`}
            />
            <FormControlLabel
              style={{ userSelect: "none" }}
              control={
                <Checkbox
                  style={{ color: "#57b6b2" }}
                  checked={statusFilter.unchangedObjects}
                  onChange={handleFilterChange}
                  name="unchangedObjects"
                />
              }
              label={`Unchanged Objects (${
                props.dbComparison.results.filter(
                  (obj) => obj._action === "NO CHANGE"
                ).length
              })`}
            />
          </FormGroup>
        </div>
        <div
          id="myGrid"
          style={{
            height: "100%",
            width: "100%",
          }}
          className="ag-theme-alpine"
        >
          <AgGridReact
            frameworkComponents={{
              buttonCellRenderer: ButtonCellRenderer,
              diffCellRenderer: DiffCellRenderer,
            }}
            onGridReady={onGridReady}
            defaultColDef={{
              flex: 1,
              minWidth: 100,
              resizable: true,
              headerCheckboxSelection: isFirstColumn,
              checkboxSelection: isFirstColumn,
              headerCheckboxSelectionFilteredOnly: true,
              filter: true,
              filterParams: { buttons: ["reset", "apply"], closeOnApply: true },
            }}
            suppressRowClickSelection={true}
            rowSelection={"multiple"}
            rowData={props.dbComparison.results}
            isExternalFilterPresent={isExternalFilterPresent}
            doesExternalFilterPass={doesExternalFilterPass}
            enableCellTextSelection={true}
            onSelectionChanged={handleSelectionChanged}
            isRowSelectable={function (rowNode) {
              return rowNode.data ? rowNode.data.status !== "NO CHANGE" : false;
            }}
          >
            {/* <AgGridColumn
                            headerName="Status"
                            field="status"
                            minWidth={180}
                            headerCheckboxSelection={true}
                            headerCheckboxSelectionFilteredOnly={true}
                            // checkboxSelection={true}
                            checkboxSelection={true}                            
                        /> */}
            <AgGridColumn field="_action" headerName="Action" />
            <AgGridColumn field="schema_name" headerName="Schema" />
            <AgGridColumn field="db_object_type" headerName="Object Type" />
            <AgGridColumn field="object_name" headerName="Object Name" />
            <AgGridColumn
              field="last_altered"
              headerName="Source Last Altered"
              valueFormatter={noValueFormatter}
            />
            <AgGridColumn
              field="last_altered"
              headerName="Target Last Altered"
              valueFormatter={noValueFormatter}
            />
            <AgGridColumn
              field="change_ddl"
              headerName=""
              cellRenderer="buttonCellRenderer"
            />
            <AgGridColumn
              field="diff_ddl"
              headerName=""
              cellRenderer="diffCellRenderer"
            />
          </AgGridReact>

          <FormGroup className={classes.root} row>
            <TextField
              required
              id="standard-required"
              label="Jira Number"
              variant="outlined"
              value={jiraNumber}
              onInput={(event) => setJiraNumber(event.target.value)}
            />
            <Button
              style={{ backgroundColor: "#57b6b2" }}
              onClick={generateDDL}
              disabled={selectedCount == 0 ? "disabled" : undefined}
            >
              Download DDL ({selectedCount})
            </Button>
          </FormGroup>
        </div>
      </div>
    </div>
  );
};

function noValueFormatter(params) {
  if (params.value === "None") {
    return "NA";
  } else {
    let dt = new Date(params.value);
    return `${dt.toLocaleDateString()} ${dt.toLocaleTimeString()}`;
  }
}

function isFirstColumn(params) {
  var displayedColumns = params.columnApi.getAllDisplayedColumns();
  var thisIsFirstColumn = displayedColumns[0] === params.column;
  return thisIsFirstColumn;
}

export default AGGridResults;
