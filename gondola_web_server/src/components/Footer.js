import React from 'react';
import CssBaseline from '@material-ui/core/CssBaseline';
import Typography from '@material-ui/core/Typography';
import { makeStyles } from '@material-ui/core/styles';
import Container from '@material-ui/core/Container';
import Link from '@material-ui/core/Link';

function Copyright() {
  return (
    <Typography variant="body2" color="textSecondary">
      {'Copyright © '}
      <Link color="inherit" href="https://material-ui.com/">
        Your Website
      </Link>{' '}
      {new Date().getFullYear()}
      {'.'}
    </Typography>
  );
}

const useStyles = makeStyles((theme) => ({
  root: {
    display: 'flex',
    flexDirection: 'column',
    // minHeight: '100vh',
  },
  main: {
    marginTop: theme.spacing(8),
    marginBottom: theme.spacing(2),
  },
  footer: {
    padding: theme.spacing(3, 2),
    marginTop: 'auto',
    backgroundColor:
      theme.palette.type === 'light' ? theme.palette.grey[200] : theme.palette.grey[800],
  },
}));

export default function StickyFooter() {
  const classes = useStyles();

  return (
    <div className={classes.root}>
      <CssBaseline />
      <footer className={classes.footer}>
        <Container maxWidth="sm">
        <div class="card text-center">
              <div class="card-header">
                &nbsp;
              </div>
              <div class="card-body">
                <h5 class="card-title"> Gondola </h5>
                <em><p class="card-text">Gondola has been created by DTSQUARED and integrates with Snowflake to provide a seamless cloud release process.</p></em>
                <div><em>DTSQUARED, 80&#x2013;83 Long Lane, London, EC1A 9ET</em></div>
                <div><span>&nbsp;&nbsp;</span></div>
                <a href="https://www.snowflake.com/"><img src='snowflake.png' alt="SnowFlake" width="60" height="50"/></a>
                <span>&nbsp;&nbsp;</span>
                <a href="https://www.dtsquared.co.uk/"><img src='dtsquared.png' alt="DTSQUARED" width="105" height="40"/></a>
              </div>
              <div class="card-footer text-muted">
                DTSQUARED 2021
              </div>
            </div>
          <Copyright />
        </Container>
      </footer>
    </div>
  );
}