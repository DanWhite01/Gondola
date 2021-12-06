import React from 'react';
import { makeStyles } from '@material-ui/core/styles';
import FormControl from '@material-ui/core/FormControl';

import FormLabel from '@material-ui/core/FormLabel';
import FormGroup from '@material-ui/core/FormGroup';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import Checkbox from '@material-ui/core/Checkbox';
import Button from '@material-ui/core/Button';
import Box from '@material-ui/core/Box';
import Divider from '@material-ui/core/Divider';
import RefreshIcon from '@material-ui/icons/Refresh';

import TextField from '@material-ui/core/TextField';
import Autocomplete from '@material-ui/lab/Autocomplete';
import Popper from "@material-ui/core/Popper";
import BeatLoader from "react-spinners/BeatLoader";
import Dialog from '@material-ui/core/Dialog';
import DialogTitle from '@material-ui/core/DialogTitle';


const useStyles = makeStyles((theme) => ({
  formControl: {
    margin: theme.spacing(1),
    minWidth: 120,
  },
  selectEmpty: {
    marginTop: theme.spacing(2),
  },
  buttonControl: {
    marginTop: 10,
    marginLeft: 10,
    marginRight: 10,
    backgroundColor: "#57b6b2"
  },
  popper: {
    maxWidth: "fit-content"
  },
}));

const PopperMy = function (props) {
  return <Popper {...props} style={useStyles.popper} placement="bottom-start" />;
};

function Selection(props) {

  const classes = useStyles();
  const [loading, setLoading] = React.useState(false);


  const [filterSelectionList, setFilterSelectionList] = React.useState({
    schema: false,
    tables: false,
    views: false,
    // columns: false,
    sequences: false,
    procedures: false,
    functions: false,
    file_formats: false,
    //stages: false,
    pipes: false,
    streams: false,
    tasks: false,
  });

  const handleFilterSelectionChange = (event) => {
    setFilterSelectionList({ ...filterSelectionList, [event.target.name]: event.target.checked });
    if (event.target.checked) allFilterSelection["checked"] = true; 
  };

  const [allFilterSelection, setAllFilterSelection] = React.useState({
    checked: false,
  });

  const handleAllFilterSelectionChange = (event) => {
    var changedVal = event.target.checked;
    setAllFilterSelection({ ...allFilterSelection, [event.target.name]: event.target.checked });
    Object.keys(filterSelectionList).forEach((key) => {
      filterSelectionList[key] = changedVal;
    });
  }

  const [dbList, setDbList] = React.useState([]);
  const [srcDbName, setSrcDbName] = React.useState('');
  const [tgtDbName, setTgtDbName] = React.useState('');

  const enableCompareButton = () => {
    if ((srcDbName !== '' || srcDbName === "null") && (tgtDbName !== '' || tgtDbName === "null") && allFilterSelection["checked"]) return true;
    return false;
  }

  React.useEffect(() => {
    var requestOptions = {
      method: 'GET',
      redirect: 'follow'
    };

    setLoading(true);

    fetch(process.env.REACT_APP_API_URL + "/5", requestOptions)
      .then(response => response.json())
      .then(result => setDbList(result))
      .then(() => setLoading(false))
      .catch(error => console.log('error', error));

    setSrcDbName('')
    setTgtDbName('');

  }, []); // <-- Have to pass in [] here!

  const getDBNames = () => {
    var requestOptions = {
      method: 'GET',
      redirect: 'follow'
    };

    setLoading(true);

    fetch(process.env.REACT_APP_API_URL + "/5", requestOptions)
      .then(response => response.json())
      .then(result => setDbList(result))
      .then(() => setLoading(false))
      .catch(error => console.log('error', error));

    setSrcDbName('')
    setTgtDbName('');
  };

  const updateResults = async () => {

    let dbComparison = { srcDbName: '', tgtDbName: '', loading: true, results: [] }
    props.updateDbComparison(dbComparison)

    var compareResult = [];

    var formdata = new FormData();
    formdata.append("src_db", srcDbName);
    formdata.append("tgt_db", tgtDbName);
    formdata.append("object", Object.keys(filterSelectionList).filter(k => filterSelectionList[k] === true));

    // for (var pair of formdata.entries()) {
    //   console.log(pair[0] + ', ' + pair[1]);
    // }
    var requestOptions = {
      method: 'PUT',
      body: formdata,
      redirect: 'follow'
    };

    setLoading(true);

    await fetch(process.env.REACT_APP_API_URL + "/0", requestOptions)
      .then(response => response.json())
      .catch(error => console.log('error', error));

    requestOptions = {
      method: 'GET',
      redirect: 'follow'
    };

    await fetch(process.env.REACT_APP_API_URL + "/99", requestOptions)
      .then(response => response.json())
      .then(result => compareResult = result)
      .catch(error => console.log('error', error));

    setLoading(false);

    dbComparison = { srcDbName: srcDbName, tgtDbName: tgtDbName, loading: false, results: compareResult }

    props.updateDbComparison(dbComparison)

  }

  return (
    <div >

      <Box
        display="flex"
        justifyContent="center"
        pt={2}
      >
        <FormControl variant="outlined" className={classes.formControl} style={{ width: '94%' }}>
          <Autocomplete
            id="srcDb-autocompiete"
            options={dbList}
            PopperComponent={PopperMy}
            value={srcDbName}
            onChange={(event, newValue) => {
              setSrcDbName(newValue)
              console.log("newValue = " + newValue)
            }}
            getOptionLabel={(option) => {
              return option;
            }}
            style={{
              width: '100%'
            }}
            renderInput={(params) => (
              <TextField {...params} label="Source Database" variant="outlined" />
            )}
          />
        </FormControl>
      </Box>
      <Box
        display="flex"
        justifyContent="center"
      >
        <FormControl variant="outlined" className={classes.formControl} style={{ width: '94%' }}>
          <Autocomplete
            id="tgtDb-autoselect"
            options={dbList}
            PopperComponent={PopperMy}
            value={tgtDbName}
            onChange={(event, newValue) => {
              setTgtDbName(newValue)
            }}
            getOptionLabel={(option) => {
              return option;
            }}
            style={{
              width: '100%'
            }}
            renderInput={(params) => (
              <TextField {...params} label="Target Database" variant="outlined" />
            )}
          />
        </FormControl>

      </Box>

      <Divider variant="fullWidth" />

      <Box
        display="flex"
        alignContent="flex-center"
        pt={1}
        pb={1}
      >
        <FormControl component="fieldset" className={classes.formControl}>
          <FormControlLabel
            control={<Checkbox style={{ color: "#57b6b2" }} checked={allFilterSelection["checked"]} onChange={handleAllFilterSelectionChange} name={"checked"} />}
            label={"Select Objects to compare"}
          />
          <Divider variant="fullWidth" />
          <FormGroup>
            {
              Object.keys(filterSelectionList).map(filterSelectionItem =>
                <FormControlLabel
                  control={<Checkbox style={{ color: "#57b6b2" }} checked={filterSelectionList[filterSelectionItem]} onChange={handleFilterSelectionChange} name={filterSelectionItem} />}
                  label={filterSelectionItem}
                />
              )
            }
          </FormGroup>
        </FormControl>
      </Box>
      <Box
        display="flex"
        justifyContent="center"
        pb={3}
      >
        <Button className={classes.buttonControl} onClick={() => updateResults()} variant="contained" disabled={enableCompareButton() ? undefined : 'disabled'} >Compare Objects</Button>
      </Box>
      <Divider variant="fullWidth" />
      <Box
        display="flex"
        justifyContent="center"
        pb={3}
        pt={1}
      >
        <Button startIcon={<RefreshIcon />} style={{ color: "#FFFFFE", backgroundColor: "#07BBF7" }} className={classes.buttonControl} variant="contained" onClick={() => { getDBNames(); }}>Refresh DB List</Button>
      </Box>

      <Divider variant="fullWidth" />


      {loading && <Dialog
        open={loading}
        onClose={loading}
        aria-labelledby="max-width-dialog-title"
      >
        <DialogTitle id="max-width-dialog-title" className={classes.root}>Loading <BeatLoader /></DialogTitle>

      </Dialog>}

    </div>
  );
}

export default Selection;