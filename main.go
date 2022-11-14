package main

import (
	"cloud.google.com/go/datastore"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/driveactivity/v2"
	"google.golang.org/api/option"
	"log"
	"net/http"
	"os"
	"reflect"
	"strings"
)

// Retrieve a token, saves the token, then returns the generated client.
func getClient(config *oauth2.Config) *http.Client {
	// The file token.json stores the user's access and refresh tokens, and is
	// created automatically when the authorization flow completes for the first
	// time.
	tokFile := "token.json"
	tok, err := tokenFromFile(tokFile)
	if err != nil {
		tok = getTokenFromWeb(config)
		saveToken(tokFile, tok)
	}
	return config.Client(context.Background(), tok)
}

// Request a token from the web, then returns the retrieved token.
func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the "+
		"authorization code: \n%v\n", authURL)

	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		log.Fatalf("Unable to read authorization code: %v", err)
	}

	tok, err := config.Exchange(context.TODO(), authCode)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web: %v", err)
	}
	return tok
}

// Retrieves a token from a local file.
func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	return tok, err
}

// Saves a token to a file path.
func saveToken(path string, token *oauth2.Token) {
	fmt.Printf("Saving credential file to: %s\n", path)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatalf("Unable to cache oauth token: %v", err)
	}
	defer f.Close()
	json.NewEncoder(f).Encode(token)
}

// Returns a string representation of the first elements in a list.
func truncated(array []string) string {
	return truncatedTo(array, 2)
}

// Returns a string representation of the first elements in a list.
func truncatedTo(array []string, limit int) string {
	var contents string
	var more string
	if len(array) <= limit {
		contents = strings.Join(array, ", ")
		more = ""
	} else {
		contents = strings.Join(array[0:limit], ", ")
		more = ", ..."
	}
	return fmt.Sprintf("[%s%s]", contents, more)
}

// Returns the name of a set property in an object, or else "unknown".
func getOneOf(m interface{}) string {
	v := reflect.ValueOf(m)
	for i := 0; i < v.NumField(); i++ {
		if !v.Field(i).IsNil() {
			return v.Type().Field(i).Name
		}
	}
	return "unknown"
}

// Returns a time associated with an activity.
func getTimeInfo(activity *driveactivity.DriveActivity) string {
	if activity.Timestamp != "" {
		return activity.Timestamp
	}
	if activity.TimeRange != nil {
		return activity.TimeRange.EndTime
	}
	return "unknown"
}

// Returns the type of action.
func getActionInfo(action *driveactivity.ActionDetail) string {
	return getOneOf(*action)
}

// Returns user information, or the type of user if not a known user.
func getUserInfo(user *driveactivity.User) string {
	if user.KnownUser != nil {
		if user.KnownUser.IsCurrentUser {
			return "people/me"
		}
		return user.KnownUser.PersonName
	}
	return getOneOf(*user)
}

// Returns actor information, or the type of actor if not a user.
func getActorInfo(actor *driveactivity.Actor) string {
	if actor.User != nil {
		return getUserInfo(actor.User)
	}
	return getOneOf(*actor)
}

// Returns information for a list of actors.
func getActorsInfo(actors []*driveactivity.Actor) []string {
	actorsInfo := make([]string, len(actors))
	for i := range actors {
		actorsInfo[i] = getActorInfo(actors[i])
	}
	return actorsInfo
}

// Returns the type of a target and an associated title.
func getTargetInfo(target *driveactivity.Target) string {
	if target.DriveItem != nil {
		return fmt.Sprintf("driveItem:\"%s\"", target.DriveItem.Title)
	}
	if target.Drive != nil {
		return fmt.Sprintf("drive:\"%s\"", target.Drive.Title)
	}
	if target.FileComment != nil {
		parent := target.FileComment.Parent
		if parent != nil {
			return fmt.Sprintf("fileComment:\"%s\"", parent.Title)
		}
		return "fileComment:unknown"
	}
	return getOneOf(*target)
}

// Returns information for a list of targets.
func getTargetsInfo(targets []*driveactivity.Target) []string {
	targetsInfo := make([]string, len(targets))
	for i := range targets {
		targetsInfo[i] = getTargetInfo(targets[i])
	}
	return targetsInfo
}

func main() {
	ctx := context.Background()
	cred := os.Getenv("GWS_CREDENTIALS_PATH")
	b, err := os.ReadFile(cred)
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	// If modifying these scopes, delete your previously saved token.json.
	config, err := google.ConfigFromJSON(b, driveactivity.DriveActivityReadonlyScope)
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}
	client := getClient(config)

	srv, err := driveactivity.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		log.Fatalf("Unable to retrieve driveactivity Client %v", err)
	}

	q := driveactivity.QueryDriveActivityRequest{PageSize: 10}
	r, err := srv.Activity.Query(&q).Do()
	if err != nil {
		log.Fatalf("Unable to retrieve list of activities. %v", err)
	}

	project := os.Getenv("PROJECT_ID")
	storeCli, err := newDataStoreClient(ctx, project)
	if err != nil {
		log.Fatalf("datastore client error: %v\n", err)
	}
	defer storeCli.close()

	fmt.Println("Recent Activity:")
	if len(r.Activities) > 0 {
		for _, a := range r.Activities {
			time := getTimeInfo(a)
			action := getActionInfo(a.PrimaryActionDetail)
			actors := getActorsInfo(a.Actors)
			targets := getTargetsInfo(a.Targets)

			fmt.Printf("%s: %s, %s, %s\n", time, truncated(actors), action, truncated(targets))

			digest := getSHA256Binary(time + action + actors[0] + targets[0])
			fmt.Printf("Digest key: %s\n", digest)

			event := ExtractedEvent{}
			storeCli.generateNameKey("gws-event", digest)
			err = storeCli.get(ctx, event)
			if errors.Is(err, datastore.ErrNoSuchEntity) {
				event = ExtractedEvent{
					Time:    time,
					Action:  action,
					Actors:  actors,
					Targets: targets,
				}
				if err = storeCli.put(ctx, event); err != nil {
					log.Fatalf("put record error datastore:%v\n", err)
				}
			} else if err != nil {
				log.Fatalf("get record error from datastore:%v\n", err)
			} else {
				log.Printf("getted record exists at datastore:%v\n", storeCli.taskKey.Name)
			}
		}
	} else {
		fmt.Print("No activity.")
	}

}
func getSHA256Binary(s string) string {
	r := sha256.Sum256([]byte(s))
	return hex.EncodeToString(r[:])
}

type ExtractedEvent struct {
	Time    string   `json:"time"`
	Action  string   `json:"action"`
	Actors  []string `json:"actors"`
	Targets []string `json:"targets"`
}

type dataStoreClient struct {
	client  *datastore.Client
	taskKey *datastore.Key
}

func newDataStoreClient(ctx context.Context, project string) (dataStoreClient, error) {
	cli, err := datastore.NewClient(ctx, project)
	if err != nil {
		log.Printf("Failed to create datastore client: %v", err)
		return dataStoreClient{}, err
	}
	return dataStoreClient{
		client: cli,
	}, nil
}

func (dc *dataStoreClient) generateIDKey(kind string, id int64) {
	dc.taskKey = datastore.IDKey(kind, id, nil)
}

func (dc *dataStoreClient) generateNameKey(kind string, name string) {
	dc.taskKey = datastore.NameKey(kind, name, nil)
}

func (dc *dataStoreClient) put(ctx context.Context, entity ExtractedEvent) error {
	//fmt.Println("data store put", entity)
	if _, err := dc.client.Put(ctx, dc.taskKey, &entity); err != nil {
		log.Printf("Failed to save entity: %v\n", err)
		return err
	}
	return nil
}

func (dc *dataStoreClient) get(ctx context.Context, entity ExtractedEvent) error {
	//fmt.Println("get key: ", dc.taskKey.Name)
	if err := dc.client.Get(ctx, dc.taskKey, &entity); err != nil {
		return err
	}
	//fmt.Println("data store get result", entity)
	return nil
}

func (dc *dataStoreClient) close() {
	dc.client.Close()
}
