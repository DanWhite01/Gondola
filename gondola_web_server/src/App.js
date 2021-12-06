//import logo from './logo.svg';
import "./App.css";

import React from "react";
import { makeStyles } from "@material-ui/core/styles";
import Drawer from "@material-ui/core/Drawer";
import AppBar from "@material-ui/core/AppBar";
import CssBaseline from "@material-ui/core/CssBaseline";
import Toolbar from "@material-ui/core/Toolbar";
import Typography from "@material-ui/core/Typography";

import Selection from "./components/Selection";

import AGGridResults from "./components/AGGridResults";

const drawerWidth = 250;

const useStyles = makeStyles((theme) => ({
  root: {
    display: "flex",
  },
  appBar: {
    zIndex: theme.zIndex.drawer + 1,
    backgroundColor: "#003057",
    outlineColor: "#57B6B2",
    outlineStyle: "solid",
    outlineWidth: "0px",
    height: "100",
  },
  drawer: {
    width: drawerWidth,
    flexShrink: 0,
  },
  drawerPaper: {
    width: drawerWidth,
  },
  drawerContainer: {
    overflow: "auto",
  },
  content: {
    flexGrow: 1,
    padding: theme.spacing(3),
  },
  toolbar: {
    alignItems: "flex-start",
    paddingTop: theme.spacing(1),
    paddingBottom: theme.spacing(2),
  },
}));

function App() {
  // set initial state to empty array
  const [dbComparison, setDbComparison] = React.useState({
    srcDbName: "",
    tgtDbName: "",
    results: [],
  });
  const [loadingIndicator, setLoadingIndicator] = React.useState(false);
  const classes = useStyles();

  return (
    <div className={classes.root}>
      <CssBaseline />

      {/* app bar */}
      <AppBar position="fixed" className={classes.appBar}>
        <Toolbar>
          <Typography variant="h6" noWrap>
            <img src="./Gondalacol.png" alt="icon" width="140" height="40" />
          </Typography>
        </Toolbar>
      </AppBar>

      {/* This is the drawer on the LHS where the selection pane goes */}
      <Drawer
        className={classes.drawer}
        variant="permanent"
        classes={{
          paper: classes.drawerPaper,
        }}
      >
        {/* Add toolbar to get top padding correct?! */}
        <Toolbar className={classes.toolbar} />

        {/* This is the actual selection component.  We pass the callback function setDbComparisonResults as a prop so that the
            selection pane can update the results which are then passed to the Results component*/}
        <Selection
          className={classes.drawerContainer}
          updateDbComparison={setDbComparison}
        />
      </Drawer>

      {/* this is where the results go */}
      <main className={classes.content}>
        <Toolbar />
        {dbComparison.srcDbName !== null && dbComparison.tgtDbName && (
          <Typography paragraph>
            Source Database : {dbComparison.srcDbName} Target Database :{" "}
            {dbComparison.tgtDbName}
          </Typography>
        )}
        {/* This is the actual results component.  We pass the dbComparisonResults object as a prop */}
        {/* <Results dbComparisonResults={dbComparisonResults} */}

        {/* { loading ? <BeatLoader /> : <AGGridResults dbComparison={dbComparison} /> } */}
        <AGGridResults dbComparison={dbComparison} />
      </main>
    </div>
  );
}

export default App;
