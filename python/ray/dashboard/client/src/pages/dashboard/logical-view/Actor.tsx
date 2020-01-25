import Collapse from "@material-ui/core/Collapse";
import { Theme } from "@material-ui/core/styles/createMuiTheme";
import createStyles from "@material-ui/core/styles/createStyles";
import withStyles, { WithStyles } from "@material-ui/core/styles/withStyles";
import Typography from "@material-ui/core/Typography";
import React from "react";
import {
  checkProfilingStatus,
  CheckProfilingStatusResponse,
  getProfilingResultURL,
  launchProfiling,
  RayletInfoResponse
} from "../../../api";
import Actors from "./Actors";

const styles = (theme: Theme) =>
  createStyles({
    root: {
      borderColor: theme.palette.divider,
      borderStyle: "solid",
      borderWidth: 1,
      marginTop: theme.spacing(2),
      padding: theme.spacing(2)
    },
    title: {
      color: theme.palette.text.secondary,
      fontSize: "0.75rem"
    },
    action: {
      color: theme.palette.primary.main,
      textDecoration: "none",
      "&:hover": {
        cursor: "pointer"
      }
    },
    infeasible: {
      color: theme.palette.error.main
    },
    overloaded: {
      "&:not(:first-child)": {
        marginLeft: theme.spacing(2)
      },
      color: theme.palette.error.main,
      fontWeight: "bold"
    },
    information: {
      fontSize: "0.875rem"
    },
    datum: {
      "&:not(:first-child)": {
        marginLeft: theme.spacing(2)
      }
    },
    webuiDisplay: {
      fontSize: "0.875rem"
    },
    actorTitle: {
      fontWeight: "bold"
    },
    secondaryFields : {
      color: theme.palette.text.secondary,
      "&:not(:first-child)": {
        marginLeft: theme.spacing(2)
      }
    },
    secondaryFieldsHeader: {
      color: theme.palette.text.secondary
    }
  });

interface Props {
  actor: RayletInfoResponse["actors"][keyof RayletInfoResponse["actors"]];
}

interface State {
  expanded: boolean;
  profiling: {
    [profilingId: string]: {
      startTime: number;
      latestResponse: CheckProfilingStatusResponse | null;
    };
  };
}

interface InformationItem {
  label: string;
  value: string | undefined | boolean;
  rendered?: JSX.Element | undefined;
}

class Actor extends React.Component<Props & WithStyles<typeof styles>, State> {
  state: State = {
    expanded: true,
    profiling: {}
  };

  setExpanded = (expanded: boolean) => () => {
    this.setState({ expanded });
  };

  handleProfilingClick = (duration: number) => async () => {
    const actor = this.props.actor;
    if (actor.state !== -1) {
      const profilingId = await launchProfiling(
        actor.nodeId,
        actor.pid,
        duration
      );
      this.setState(state => ({
        profiling: {
          ...state.profiling,
          [profilingId]: { startTime: Date.now(), latestResponse: null }
        }
      }));
      const checkProfilingStatusLoop = async () => {
        const response = await checkProfilingStatus(profilingId);
        this.setState(state => ({
          profiling: {
            ...state.profiling,
            [profilingId]: {
              ...state.profiling[profilingId],
              latestResponse: response
            }
          }
        }));
        if (response.status === "pending") {
          setTimeout(checkProfilingStatusLoop, 1000);
        }
      };
      await checkProfilingStatusLoop();
    }
  };

  render() {
    const { classes, actor } = this.props;
    const { expanded, profiling } = this.state;

    let information : InformationItem[] =
      actor.state !== -1
        ? [
            {
              label: "ActorTitle",
              value: actor.actorTitle
            },
            {
              label: "Resources",
              value:
                Object.entries(actor.usedResources).length > 0 &&
                Object.entries(actor.usedResources)
                  .sort((a, b) => a[0].localeCompare(b[0]))
                  .map(([key, value]) => `${value.toLocaleString()} ${key}`)
                  .join(", ")
            },
            {
              label: "Pending",
              value: actor.taskQueueLength.toLocaleString()
            },
            {
              label: "Executed",
              value: actor.numExecutedTasks.toLocaleString()
            },
            {
              label: "NumObjectIdsInScope",
              value: actor.numObjectIdsInScope.toLocaleString()
            },
            {
              label: "NumLocalObjects",
              value: actor.numLocalObjects.toLocaleString()
            },
            {
              label: "UsedLocalObjectMemory",
              value: actor.usedObjectStoreMemory.toLocaleString()
            },
            {
              label: "Task",
              value: actor.currentTaskFuncDesc.join(".")
            }
          ]
        : [
            {
              label: "ID",
              value: actor.actorId
            },
            {
              label: "Required resources",
              value:
                Object.entries(actor.requiredResources).length > 0 &&
                Object.entries(actor.requiredResources)
                  .sort((a, b) => a[0].localeCompare(b[0]))
                  .map(([key, value]) => `${value.toLocaleString()} ${key}`)
                  .join(", ")
            }
          ];
    
    if (actor.state !== -1) {
      let foundPending = information.find((object=>object.label ==="Pending"))
      if (foundPending && actor.taskQueueLength >= 50) {
        foundPending.rendered = <React.Fragment key={foundPending.label}>
        <span className={classes.overloaded}>{foundPending.label}: {foundPending.value}</span>{" "}
      </React.Fragment>
      }

      let foundActorTitle = information.find(object=>object.label==="ActorTitle")
      if (foundActorTitle) {
        foundActorTitle.rendered = <React.Fragment key={foundActorTitle.label}>
          {/* <span className={classes.datum}>{foundActorTitle.label}:</span> */}
          <span className={classes.actorTitle}> {foundActorTitle.value}</span>{" "}
        </React.Fragment>
      }

      const grayOutFields = [
        "NumObjectIdsInScope",
        "NumLocalObjects",
        "UsedLocalObjectMemory"
      ]

      grayOutFields.map((fieldName, idx, _) => {
        let found = information.find(object=>object.label===fieldName)
        if (found) {
          found.rendered = <React.Fragment key={found.label}>
            {idx === 0 && <br></br>}
            <span className={
              idx === 0 ? classes.secondaryFieldsHeader : classes.secondaryFields
              }>{found.label}: {found.value}</span>{" "}
          </React.Fragment>

          information.splice(information.indexOf(found), 1)
          information.push(found)
        }
        })
    }
    
    return (
      <div className={classes.root}>
        <Typography className={classes.title}>
          {actor.state !== -1 ? (
            <React.Fragment>
              Actor {actor.actorId}{" "}
              {Object.entries(actor.children).length > 0 && (
                <React.Fragment>
                  (
                  <span
                    className={classes.action}
                    onClick={this.setExpanded(!expanded)}
                  >
                    {expanded ? "Collapse" : "Expand"}
                  </span>
                  )
                </React.Fragment>
              )}{" "}
              (Profile for
              {[10, 30, 60].map(duration => (
                <React.Fragment>
                  {" "}
                  <span
                    className={classes.action}
                    onClick={this.handleProfilingClick(duration)}
                  >
                    {duration}s
                  </span>
                </React.Fragment>
              ))}
              ){" "}
              {Object.entries(profiling).map(
                ([profilingId, { startTime, latestResponse }]) =>
                  latestResponse !== null && (
                    <React.Fragment>
                      (
                      {latestResponse.status === "pending" ? (
                        `Profiling for ${Math.round(
                          (Date.now() - startTime) / 1000
                        )}s...`
                      ) : latestResponse.status === "finished" ? (
                        <a
                          className={classes.action}
                          href={getProfilingResultURL(profilingId)}
                          rel="noopener noreferrer"
                          target="_blank"
                        >
                          Profiling result
                        </a>
                      ) : latestResponse.status === "error" ? (
                        `Profiling error: ${latestResponse.error.trim()}`
                      ) : (
                        undefined
                      )}
                      ){" "}
                    </React.Fragment>
                  )
              )}
            </React.Fragment>
          ) : (
            <span className={classes.infeasible}>Infeasible actor</span>
          )}
        </Typography>
        <Typography className={classes.information}>
          {information.map(
            ({ label, value, rendered }) => {
              return rendered ||
                value && value.toString().length > 0 && (
                  <React.Fragment key={label.toString()}>
                    <span className={classes.datum}>
                      {label}: {value}
                    </span>{" "}
                  </React.Fragment>
                )
            }
              
          )}
        </Typography>
        {actor.state !== -1 && (
          <React.Fragment>
            {actor.webuiDisplay && (
              <Typography className={classes.webuiDisplay}>
                {actor.webuiDisplay}
              </Typography>
            )}
            <Collapse in={expanded}>
              <Actors actors={actor.children} />
            </Collapse>
          </React.Fragment>
        )}
      </div>
    );
  }
}

export default withStyles(styles)(Actor);
