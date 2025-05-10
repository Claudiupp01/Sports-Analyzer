// to compile: gcc main.c -L. -lcjson -o main
// to execute: ./main



#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <pthread.h>

#include <time.h>
#include <math.h>
#include <unistd.h>

#include "cJSON.h"


//#include <cjson/cJSON.h>

#define NUM_THREADS 8

typedef struct {
    char firstName[50];
    char middleName[50];
    char lastName[50];
    char shortName[50];
    int wyId;
    int currentTeamId;
    int currentNationalTeamId;
    char passportArea[50];
    int weight;
    int height;
    char birthDate[20];
    char birthArea[50];
    char foot[10];
    char roleName[50];
} PLAYER_PROFILE;

typedef struct{
    int playerId;
    int assists;
    int goals;
    int ownGoals;
    int redCards;
    int yellowCards;
}PLAYER;

typedef struct{
    int refereeId;
    char role[101];
}REFEREE;

typedef struct {
    int playerIn;
    int playerOut;
    int minute;
} SUBSTITUTIONS;

typedef struct{
    int teamId;
    int finalScore;
    int coachId;
    int hasFormation;
    int benchCount;
    PLAYER bench[20];
    int lineupCount;
    PLAYER lineup[11]; 
    int substitutionsCount;
    SUBSTITUTIONS substitutions[10];
}TEAM;

typedef struct{
    char type[101];
    int wyId;
    int roundId;
    int gameweek;
    char status[10];
    TEAM teams[2];
    REFEREE referee_data[4];
} MATCH;

typedef enum {
    Player,
    Match
} DataType;

typedef union {
    PLAYER_PROFILE player;
    MATCH match;
} BufferData;

typedef struct {
    int thread_id;
    int start_idx;
    int end_idx;
    DataType data_type;
    PLAYER_PROFILE *players;
    int player_count;
    MATCH *matches;
    int match_count;
} ProducerArgs;

#define MAX_TIMESTAMPS 1000
#define MAX_THREAD_NAME_LENGTH 100

struct {
    struct timespec timestamps[MAX_TIMESTAMPS];
    char thread_names[MAX_TIMESTAMPS][MAX_THREAD_NAME_LENGTH];
    int count;
    pthread_mutex_t mutex;
} timing_data = {{0}, {{0}}, 0, PTHREAD_MUTEX_INITIALIZER};


void log_elapsed_time(const char *thread_name, struct timespec start, struct timespec finish) 
{
    double elapsed;

    clock_gettime(CLOCK_MONOTONIC, &finish);

    elapsed = (finish.tv_sec - start.tv_sec);
    elapsed += ((finish.tv_nsec - start.tv_nsec) / 1000000000.0);

    pthread_mutex_lock(&timing_data.mutex);
    if (timing_data.count < MAX_TIMESTAMPS) 
    {
        timing_data.timestamps[timing_data.count] = finish;
        snprintf(timing_data.thread_names[timing_data.count], MAX_THREAD_NAME_LENGTH, "%s (Elapsed: %.12f s)", thread_name, elapsed);
        timing_data.count++;
    }
    pthread_mutex_unlock(&timing_data.mutex);
}

int stop_profiling = 0;

void *profiling_thread_func(void *arg)
{
    FILE *file = fopen("timing_data.txt", "w");

    if (file == NULL) 
    {
        perror("Unable to open timing data file");
        pthread_exit(NULL);
    }

    while (stop_profiling != 1) 
    {
        //fprintf(file, "Profiling thread active: logging %d entries\n", timing_data.count);
        pthread_mutex_lock(&timing_data.mutex);
        if(timing_data.count > 0)
        {
            fprintf(file, "Profiling thread active: logging %d entries\n", timing_data.count);
        }
        for (int i = 0; i < timing_data.count; i++) 
        {
            fprintf(file, "Source: %s, Timestamp: %ld.%09ld\n",
                    timing_data.thread_names[i],
                    timing_data.timestamps[i].tv_sec,
                    timing_data.timestamps[i].tv_nsec);
        }
        timing_data.count = 0;

        pthread_mutex_unlock(&timing_data.mutex);
        
        fflush(file);
    }

    fclose(file);
    pthread_exit(NULL);
}

pthread_cond_t not_empty_cv;

pthread_cond_t not_full_cv;

pthread_mutex_t empty_mutex;

pthread_cond_t empty_cond;

pthread_mutex_t lock;

pthread_cond_t canBeginHandlingScores;

typedef struct node {
    DataType type;     
    BufferData data;   
    struct node *next;
} QUEUE_NODE;

QUEUE_NODE *head = NULL;
QUEUE_NODE *tail = NULL;

int isEmpty(void)
{
    return (head == NULL);
}

void put(DataType type, BufferData data) 
{
    QUEUE_NODE *newNode = (QUEUE_NODE *)malloc(sizeof(QUEUE_NODE));
    newNode->type = type;
    newNode->data = data;
    newNode->next = NULL;

    struct timespec start;
    struct timespec finish;
    clock_gettime(CLOCK_MONOTONIC, &start);

    pthread_mutex_lock(&lock);
    if (!isEmpty()) 
    {
        tail->next = newNode;
        tail = newNode;
    } 
    else 
    {
        head = newNode;
        tail = newNode;
    }
    pthread_cond_signal(&empty_cond);
    pthread_mutex_unlock(&lock);

    if (type == Player) 
    {
        printf("Main_Producer: Added Player: %s %s (ID: %d)\n", data.player.firstName, data.player.lastName, data.player.wyId);
    } 
    else 
    {
        if (type == Match) 
        {
            printf("Main_Producer: Added Match: ID %d, Status: %s\n", data.match.wyId, data.match.status);
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &finish);

    log_elapsed_time("Main_Producer put", start, finish);
}

/*
BufferData get(DataType *type) {
    pthread_mutex_lock(&lock);

    while (isEmpty()) {
        pthread_cond_wait(&empty_cond, &lock);
    }

    QUEUE_NODE *oldhead = head;
    *type = oldhead->type;
    BufferData data = oldhead->data;
    head = head->next;
    if (head == NULL) {
        tail = NULL;
    }
    free(oldhead);
    pthread_mutex_unlock(&lock);

    return data;
}
*/

#define MAX_SUBSTITUTIONS 10

void parse_jsonPlayer(cJSON *json, PLAYER *player)
{
    if (!json || !player) return;

    //printf("FFFFF in PLAYER \n");

    cJSON *playerIdItem = cJSON_GetObjectItem(json, "playerId");
    cJSON *assistsItem = cJSON_GetObjectItem(json, "assists");
    cJSON *goalsItem = cJSON_GetObjectItem(json, "goals");
    cJSON *ownGoalsItem = cJSON_GetObjectItem(json, "ownGoals");
    cJSON *redCardsItem = cJSON_GetObjectItem(json, "redCards");
    cJSON *yellowCardsItem = cJSON_GetObjectItem(json, "yellowCards");


    /*
    if (playerIdItem) printf("Player ID found: %d\n", playerIdItem->valueint);
    if (assistsItem) printf("Assists found: %s\n", assistsItem->valuestring);
    if (goalsItem) printf("Goals found: %s\n", goalsItem->valuestring);
    if (ownGoalsItem) printf("Own Goals found: %s\n", ownGoalsItem->valuestring);
    if (redCardsItem) printf("Red Cards found: %s\n", redCardsItem->valuestring);
    if (yellowCardsItem) printf("Yellow Cards found: %s\n", yellowCardsItem->valuestring);
    */

    
    if (playerIdItem)
    {
        player->playerId = playerIdItem->valueint;
    }
    if (assistsItem) 
    {
        player->assists = atoi(assistsItem->valuestring);
    }

    if (goalsItem && strcmp(goalsItem->valuestring, "null") != 0) 
    {
        player->goals = atoi(goalsItem->valuestring);
    } 
    else 
    {
        player->goals = 0;
    }

    if (ownGoalsItem) 
    {
        player->ownGoals = atoi(ownGoalsItem->valuestring);
    }
    if (redCardsItem) 
    {
        player->redCards = atoi(redCardsItem->valuestring);
    }
    if (yellowCardsItem) 
    {
        player->yellowCards = atoi(yellowCardsItem->valuestring);
    }
}

void parse_substitutions(cJSON *subs_json, TEAM *team)
{
    //printf("GGGGG in SUBSTITUTIONS \n");
    if (!subs_json || !team) return;

    int subs_count = 0;
    cJSON *sub = NULL;

    cJSON_ArrayForEach(sub, subs_json) 
    {
        if (subs_count >= MAX_SUBSTITUTIONS) break;

        cJSON *playerInItem = cJSON_GetObjectItem(sub, "playerIn");
        cJSON *playerOutItem = cJSON_GetObjectItem(sub, "playerOut");
        cJSON *minuteItem = cJSON_GetObjectItem(sub, "minute");

        if (playerInItem && playerOutItem && minuteItem) 
        {
            team->substitutions[subs_count].playerIn = playerInItem->valueint;
            team->substitutions[subs_count].playerOut = playerOutItem->valueint;
            team->substitutions[subs_count].minute = minuteItem->valueint;
            subs_count++;
        }
    }
    team->substitutionsCount = subs_count;
}

void parse_formation(cJSON *team_json, TEAM *team)
{
    //printf("SSSSS in FORMATION \n");
    //char *json_string = cJSON_PrintUnformatted(formation_json);
    //printf("%s \n\n\n\n\n", json_string);
    if (!team_json || !team) return; // Null check

    // Parse bench
    cJSON *formation_json = cJSON_GetObjectItem(team_json, "formation");
    //char *json_string = cJSON_PrintUnformatted(formation_json);
    //printf("%s \n\n", json_string);

    cJSON *bench = cJSON_GetObjectItem(formation_json, "bench");
    //json_string = cJSON_PrintUnformatted(bench);
    //printf("%s \n\n", json_string);

    if (bench) 
    {
        int bench_count = 0;
        cJSON *bench_player = NULL;
        cJSON_ArrayForEach(bench_player, bench) 
        {
            parse_jsonPlayer(bench_player, &team->bench[bench_count++]);
        }
        team->benchCount = bench_count;
    }

    cJSON *lineup = cJSON_GetObjectItem(formation_json, "lineup");
    if (lineup) 
    {
        int lineup_count = 0;
        cJSON *lineup_player = NULL;
        cJSON_ArrayForEach(lineup_player, lineup) 
        {
            parse_jsonPlayer(lineup_player, &team->lineup[lineup_count++]);
        }
        team->lineupCount = lineup_count;
    }

    cJSON *substitutions = cJSON_GetObjectItem(formation_json, "substitutions");
    if (substitutions) 
    {
        parse_substitutions(substitutions, team);
    }
}

void parse_match(cJSON *match_data, MATCH *match)
{
    //printf("QQQQQ in MATCH \n");
    if (!match_data || !match) return; // Null check

    cJSON *wyIdItem = cJSON_GetObjectItem(match_data, "wyId");
    cJSON *roundIdItem = cJSON_GetObjectItem(match_data, "roundId");
    cJSON *gameweekItem = cJSON_GetObjectItem(match_data, "gameweek");
    cJSON *statusItem = cJSON_GetObjectItem(match_data, "status");

    if (wyIdItem)
    {
        match->wyId = wyIdItem->valueint;
    }
    if (roundIdItem)
    {
        match->roundId = roundIdItem->valueint;
    }
    if (gameweekItem)
    {
        match->gameweek = gameweekItem->valueint;
    }
    if (statusItem)
    {
        strcpy(match->status, statusItem->valuestring);
    }

    cJSON *teams_data = cJSON_GetObjectItem(match_data, "teamsData");
    //char *json_string = cJSON_PrintUnformatted(teams_data);
    //printf("%s \n\n\n\n\n", json_string);

    if (teams_data) 
    {
        int team_count = 0;
        cJSON *team_entry = NULL;
        cJSON_ArrayForEach(team_entry, teams_data) 
        {
            //char *json_string = cJSON_PrintUnformatted(team_entry);
            //printf("%s \n\n\n\n\n", json_string);
            parse_formation(team_entry, &match->teams[team_count++]);
        }
    }
}


void parse_matches_json(const char *filename, MATCH **matches, int *match_count) 
{
    //printf("WWWWW in MATCHES \n");
    FILE *file = fopen(filename, "r");
    if (!file) 
    {
        perror("Unable to open file");
        return;
    }

    fseek(file, 0, SEEK_END);
    long length = ftell(file);
    fseek(file, 0, SEEK_SET);
    char *data = malloc(length + 1);
    fread(data, 1, length, file);
    data[length] = '\0';
    fclose(file);

    cJSON *json = cJSON_Parse(data);
    if (!json) 
    {
        fprintf(stderr, "Error parsing JSON\n");
        free(data);
        return;
    }

    *match_count = cJSON_GetArraySize(json);
    *matches = malloc(sizeof(MATCH) * (*match_count));

    for (int i = 0; i < *match_count; i++) 
    {
        cJSON *match_data = cJSON_GetArrayItem(json, i);
        parse_match(match_data, &(*matches)[i]);
    }

    cJSON_Delete(json);
    free(data);
}

void parse_player_profile(cJSON *json, PLAYER_PROFILE *player) {
    
    strcpy(player->firstName, cJSON_GetObjectItem(json, "firstName")->valuestring);
    strcpy(player->middleName, cJSON_GetObjectItem(json, "middleName")->valuestring);
    strcpy(player->lastName, cJSON_GetObjectItem(json, "lastName")->valuestring);
    strcpy(player->shortName, cJSON_GetObjectItem(json, "shortName")->valuestring);
    player->wyId = cJSON_GetObjectItem(json, "wyId")->valueint;
    player->currentTeamId = cJSON_GetObjectItem(json, "currentTeamId")->valueint;
    
    cJSON *nationalTeamId = cJSON_GetObjectItem(json, "currentNationalTeamId");
    player->currentNationalTeamId = cJSON_IsNull(nationalTeamId) ? -1 : nationalTeamId->valueint;

    cJSON *passportArea = cJSON_GetObjectItem(json, "passportArea");
    strcpy(player->passportArea, cJSON_GetObjectItem(passportArea, "name")->valuestring);

    player->weight = cJSON_GetObjectItem(json, "weight")->valueint;
    player->height = cJSON_GetObjectItem(json, "height")->valueint;
    strcpy(player->birthDate, cJSON_GetObjectItem(json, "birthDate")->valuestring);

    cJSON *birthArea = cJSON_GetObjectItem(json, "birthArea");
    strcpy(player->birthArea, cJSON_GetObjectItem(birthArea, "name")->valuestring);

    strcpy(player->foot, cJSON_GetObjectItem(json, "foot")->valuestring);

    cJSON *role = cJSON_GetObjectItem(json, "role");
    strcpy(player->roleName, cJSON_GetObjectItem(role, "name")->valuestring);
}

void parse_players_json(const char *filename, PLAYER_PROFILE **players, int *player_count) {
    FILE *file = fopen(filename, "r");
    if (!file) 
    {
        perror("Unable to open file");
        return;
    }

    fseek(file, 0, SEEK_END);
    long length = ftell(file);
    fseek(file, 0, SEEK_SET);
    char *data = malloc(length + 1);
    fread(data, 1, length, file);
    data[length] = '\0';
    fclose(file);

    cJSON *json = cJSON_Parse(data);
    if (!json) 
    {
        fprintf(stderr, "Error parsing JSON\n");
        free(data);
        return;
    }

    *player_count = cJSON_GetArraySize(json);
    *players = malloc(sizeof(PLAYER_PROFILE) * (*player_count));

    for (int i = 0; i < *player_count; i++) 
    {
        cJSON *player_data = cJSON_GetArrayItem(json, i);
        parse_player_profile(player_data, &(*players)[i]);
    }

    cJSON_Delete(json);
    free(data);
}

#define REPEAT 100

pthread_mutex_t producer_done_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t producer_done_cond = PTHREAD_COND_INITIALIZER;

pthread_mutex_t buffer_done_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t buffer_done_cond = PTHREAD_COND_INITIALIZER;

pthread_mutex_t score_done_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t score_done_cond = PTHREAD_COND_INITIALIZER;


int producers_done_count = 0;

int buffer_insertions_count = 0;

void *main_producer(void *arg) {
    ProducerArgs args = *(ProducerArgs *)arg;
    BufferData data;

    if (args.data_type == Player) 
    {
       for (int i = args.start_idx; i < args.end_idx; i++) 
       {
        // start si stop aici de exemplu ca sa iau pentru fiecate inserare in bufferul meu lista .
            data.player = args.players[i];
            put(Player, data);
            printf("Producer %d: Added Player %s %s\n", args.thread_id, data.player.firstName, data.player.lastName);
       }
    } 
    else if (args.data_type == Match) 
    {
       for (int i = args.start_idx; i < args.end_idx; i++) 
       {
            data.match = args.matches[i];
            put(Match, data);
            printf("Producer %d: Added Match %d with status %s\n", args.thread_id, data.match.wyId, data.match.status);
       }
    }

    pthread_mutex_lock(&producer_done_mutex);
    producers_done_count++;
    if (producers_done_count == NUM_THREADS * 2) 
    {
        pthread_cond_signal(&producer_done_cond);
    }
    pthread_mutex_unlock(&producer_done_mutex);

    pthread_exit(NULL);
}

/*
void *consumer(void *t) 
{
    int my_id = *(int *)t;

    for (int i = 0; i < REPEAT; i++) 
    {
        DataType type;
        BufferData data = get(&type);

        if (type == Player) 
        {
            printf("Consumer %d: Processed Player %s %s, Team ID: %d, Role: %s\n",
                   my_id, data.player.firstName, data.player.lastName, data.player.currentTeamId, data.player.roleName);
        } 
        else 
        {
            if (type == Player)
            {
                printf("Consumer %d: Processed Match %d, %d vs %d, Score: %d-%d\n",
                    my_id, data.match.wyId, data.match.teams[0].teamId, data.match.teams[1].teamId, data.match.teams[0].finalScore, data.match.teams[1].finalScore);
            }
        }
    }

    pthread_exit(NULL);
}
*/

typedef struct {
    int playerId;
    char firstName[50];
    char lastName[50];
    int assists;
    int goals;
    int ownGoals;
    int redCards;
    int yellowCards;
    int score;
} PLAYER_STATS;

PLAYER_STATS *player_stats_array = NULL;

pthread_mutex_t stats_mutex = PTHREAD_MUTEX_INITIALIZER;

#define MAX_PLAYERS 1000000

void player_stats_generator() 
{
    player_stats_array = calloc(MAX_PLAYERS, sizeof(PLAYER_STATS));
    if (!player_stats_array) 
    {
        perror("Failed to allocate player_stats_array");
        pthread_exit(NULL);
    }

    QUEUE_NODE *current = head;
    while (current != NULL) 
    {
        if (current->type == Player) 
        {
            PLAYER_PROFILE player = current->data.player;
            int pid = player.wyId;

            if (pid < 0 || pid >= MAX_PLAYERS) 
            {
                fprintf(stderr, "Invalid playerId: %d\n", pid);
            } 
            else 
            {
                player_stats_array[pid].playerId = pid;
                strcpy(player_stats_array[pid].firstName, player.firstName);
                strcpy(player_stats_array[pid].lastName, player.lastName);
            }
        }
        current = current->next;
    }

    current = head;
    while (current != NULL) 
    {
        if (current->type == Match) 
        {
            MATCH match = current->data.match;
            for (int t = 0; t < 2; t++) 
            {
                TEAM team = match.teams[t];
                
                for (int p = 0; p < team.lineupCount; p++) 
                {
                    PLAYER player = team.lineup[p];
                    int pid = player.playerId;
                    if (pid < 0 || pid >= MAX_PLAYERS) 
                    {
                        fprintf(stderr, "Invalid playerId: %d\n", pid);
                        continue;
                    }
                    
                    player_stats_array[pid].assists += player.assists;
                    player_stats_array[pid].goals += player.goals;
                    player_stats_array[pid].ownGoals += player.ownGoals;
                    player_stats_array[pid].redCards += player.redCards;
                    player_stats_array[pid].yellowCards += player.yellowCards;
                }
                
                for (int p = 0; p < team.benchCount; p++) 
                {
                    PLAYER player = team.bench[p];
                    int pid = player.playerId;
                    if (pid < 0 || pid >= MAX_PLAYERS) 
                    {
                        fprintf(stderr, "Invalid playerId: %d\n", pid);
                        continue;
                    }
                    
                    player_stats_array[pid].assists += player.assists;
                    player_stats_array[pid].goals += player.goals;
                    player_stats_array[pid].ownGoals += player.ownGoals;
                    player_stats_array[pid].redCards += player.redCards;
                    player_stats_array[pid].yellowCards += player.yellowCards;
                }
            }
        }
        current = current->next;
    }

    /*
    // Calculate scores
    for (int i = 0; i < MAX_PLAYERS; i++) 
    {
        if (player_stats_array[i].playerId != 0) 
        { // assuming 0 is invalid
            player_stats_array[i].score = (player_stats_array[i].goals * 10 + player_stats_array[i].assists * 5
                                         - player_stats_array[i].ownGoals * 3 - player_stats_array[i].redCards * 20
                                         - player_stats_array[i].yellowCards *2);
        }
    }
    */

    /*
    int player_count_for_debug = 0;

    // Print the player statistics
    for (int i = 0; i < MAX_PLAYERS; i++) 
    {
        if (player_stats_array[i].playerId != 0) 
        {
            player_count_for_debug++;
            printf("Player ID: %d, Name: %s %s, Assists: %d, Goals: %d, Own Goals: %d, Red Cards: %d, Yellow Cards: %d, Score: %d\n",
                   player_stats_array[i].playerId,
                   player_stats_array[i].firstName,
                   player_stats_array[i].lastName,
                   player_stats_array[i].assists,
                   player_stats_array[i].goals,
                   player_stats_array[i].ownGoals,
                   player_stats_array[i].redCards,
                   player_stats_array[i].yellowCards,
                   player_stats_array[i].score);
        }
    }

    printf("There are %d players statistics generated. \n", player_count_for_debug);
    */
}






/* Index of queue tail (where to put next) */
int in = 0;
/* Index of queue head (where to get next) */
int out = 0;

pthread_mutex_t lockPlayerBuffer;
pthread_cond_t not_full_PlayerBuffer_cv;

#define BUFFER_SIZE 5000

PLAYER_STATS playerBuffer[BUFFER_SIZE];

void enqueue(PLAYER_STATS value)
{
    struct timespec start;
    struct timespec finish;
    clock_gettime(CLOCK_MONOTONIC, &start);

    pthread_mutex_lock(&lockPlayerBuffer);

    /* while queue is full, wait */
    /* need a while loop, not a simple if !!! */
    while (((in + 1) % BUFFER_SIZE) == out)
    {
        pthread_cond_wait(&not_full_PlayerBuffer_cv, &lockPlayerBuffer);
    }

    /* put in queue */
    playerBuffer[in] = value;
    in = (in + 1) % BUFFER_SIZE;

    /* signal that buffer is not empty */
    pthread_cond_signal(&not_empty_cv);
    pthread_mutex_unlock(&lockPlayerBuffer);

    clock_gettime(CLOCK_MONOTONIC, &finish);

    log_elapsed_time("Buffer_Producer put", start, finish);
}

typedef struct{
    int start_index;
    int end_index;
    int thread_id;
}BUFFER_ARGS;

int real_number_of_players = 0;

pthread_mutex_t number_of_players_mutex = PTHREAD_MUTEX_INITIALIZER;

void *buffer_producer(void *arg)
{
    BUFFER_ARGS args = *(BUFFER_ARGS  *)arg;

       for (int i = args.start_index; i < args.end_index; i++) 
       {
            if (player_stats_array[i].playerId != 0) 
            {
                pthread_mutex_lock(&number_of_players_mutex);
                real_number_of_players++;
                pthread_mutex_unlock(&number_of_players_mutex);
                enqueue(player_stats_array[i]);
                printf("Buffer_Producer %d: Added the statistics for player %s %s\n", 
                        args.thread_id, player_stats_array[i].firstName, player_stats_array[i].lastName);
            }
       }

    pthread_mutex_lock(&buffer_done_mutex);
    buffer_insertions_count++;
    if (buffer_insertions_count == NUM_THREADS) 
    {
        pthread_cond_signal(&buffer_done_cond);
    }
    pthread_mutex_unlock(&buffer_done_mutex);
}



int score_insertions_count = 0;

void *score_calculating_thread_function(void *arg)
{
    BUFFER_ARGS args = *(BUFFER_ARGS  *)arg;


    struct timespec start;
    struct timespec finish;
    clock_gettime(CLOCK_MONOTONIC, &start);

       for (int i = args.start_index; i < args.end_index; i++) 
       {
                pthread_mutex_unlock(&number_of_players_mutex);
                playerBuffer[i].score = (playerBuffer[i].goals * 10 + playerBuffer[i].assists * 5
                                         - playerBuffer[i].ownGoals * 3 - playerBuffer[i].redCards * 20
                                         - playerBuffer[i].yellowCards *2);
                printf("Score_Calculator %d: Calculated score for player %s %s\n", 
                        args.thread_id, playerBuffer[i].firstName, playerBuffer[i].lastName);

                clock_gettime(CLOCK_MONOTONIC, &finish);

                log_elapsed_time("Score calculator put", start, finish);
       }

    pthread_mutex_lock(&score_done_mutex);
    score_insertions_count++;
    if (score_insertions_count == NUM_THREADS)
    {
        pthread_cond_signal(&score_done_cond);
    }
    pthread_mutex_unlock(&score_done_mutex);
}

void swap(PLAYER_STATS* a, PLAYER_STATS* b) 
{
    PLAYER_STATS aux = *a;
    *a = *b;
    *b = aux;
}

int partition(PLAYER_STATS arr[], int left_index, int right_index) 
{
    PLAYER_STATS pivot = arr[right_index];
    int i = left_index - 1;

    for (int j = left_index; j <= right_index - 1; j++) 
    {
        if (arr[j].score < pivot.score) 
        {
            i++;
            swap(&arr[i], &arr[j]);
        }
    }

    swap(&arr[i + 1], &arr[right_index]);
    return i + 1;
}



void* quick_sort(void* arg) 
{
    BUFFER_ARGS* args = (BUFFER_ARGS*)arg;
    int left_index = args->start_index;
    int right_index = args->end_index;

    struct timespec start;
    struct timespec finish;
    clock_gettime(CLOCK_MONOTONIC, &start);

    if (left_index < right_index) 
    {
        pthread_t first_thread, second_thread;
        BUFFER_ARGS* info1 = (BUFFER_ARGS*)malloc(sizeof(BUFFER_ARGS));
        BUFFER_ARGS* info2 = (BUFFER_ARGS*)malloc(sizeof(BUFFER_ARGS));

        int index = partition(playerBuffer, left_index, right_index);

        PLAYER_STATS *buffer1;

        buffer1 = playerBuffer;
        info1->start_index = left_index;
        info1->end_index = index - 1;

        if (pthread_create(&first_thread, NULL, quick_sort, info1)) {
            printf("Error in creating thread\n");
            exit(-1);
        }

        PLAYER_STATS *buffer2;

        buffer2 = playerBuffer;
        info2->start_index = index + 1;
        info2->end_index = right_index;

        if (pthread_create(&second_thread, NULL, quick_sort, info2)) {
            printf("Error in creating thread\n");
            exit(-1);
        }

        clock_gettime(CLOCK_MONOTONIC, &finish);

        log_elapsed_time("Time spent in quick_sort: ", start, finish);

        pthread_join(first_thread, NULL);
        pthread_join(second_thread, NULL);

        free(info1);
        free(info2);
    }
    return NULL;
}


int main ()
{
    printf("Hello world !\n");

    MATCH *matches;
    int match_count;

    parse_matches_json("matches_World_Cup.json", &matches, &match_count);
    printf("Parsed %d matches\n", match_count);

    for (int i = 0; i < match_count; i++) 
    {
        printf("Match %d: wyId: %d, RoundId: %d, Status: %s\n",
               i + 1, matches[i].wyId, matches[i].roundId, matches[i].status);

               //for(int j=0;j<11;j++)
               //{
                //printf("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC team1: %d, team2: %d \n", matches[j].teams[0].teamId, matches[j].teams[0].teamId);
                //printf("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA player with ID: %d scored: %d goals \n", matches[i].teams[0].lineup[j].playerId, matches[i].teams[0].lineup[j].goals);
                //printf("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB player with ID: %d scored: %d goals \n", matches[i].teams[1].lineup[j].playerId, matches[i].teams[1].lineup[j].goals);
               //}
    }

    PLAYER_PROFILE *players;
    int player_count;

    parse_players_json("players.json", &players, &player_count);
    printf("Parsed %d players\n", player_count);

    for (int i = 0; i < player_count; i++) 
    {
        printf("Player %d: %s %s (ID: %d), Team ID: %d, Role: %s, Nationality: %s\n",
               i + 1, players[i].firstName, players[i].lastName, players[i].wyId,
               players[i].currentTeamId, players[i].roleName, players[i].passportArea);
    }



    pthread_t matches_producer_threads[NUM_THREADS];
    pthread_t players_producer_threads[NUM_THREADS];
    pthread_t profiler_thread;
    pthread_t statistics_producer_threads[NUM_THREADS];
    pthread_t score_calculating_threads[NUM_THREADS];

    pthread_mutex_init(&lock, NULL);
    pthread_cond_init(&empty_cond, NULL);

    pthread_mutex_init(&producer_done_mutex, NULL);
    pthread_cond_init(&producer_done_cond, NULL);

    pthread_mutex_init(&buffer_done_mutex, NULL);
    pthread_cond_init(&buffer_done_cond, NULL);

    pthread_mutex_init(&score_done_mutex, NULL);
    pthread_cond_init(&score_done_cond, NULL);



    int players_per_thread = player_count / NUM_THREADS;
    int matches_per_thread = match_count / NUM_THREADS;
    int statistics_per_thread = MAX_PLAYERS / NUM_THREADS;
    int scores_calculated_per_thread = real_number_of_players / NUM_THREADS;

    printf("In main avem player_count=%d si players_per_thread=%d \n", player_count, players_per_thread);



    pthread_create(&profiler_thread, NULL, profiling_thread_func, NULL);

for (int i = 0; i < NUM_THREADS; i++) 
    {
        ProducerArgs *args = malloc(sizeof(ProducerArgs));
        args->thread_id = i;
        args->data_type = Player;
        args->players = players;
        args->matches = matches;
        args->player_count = player_count;
        args->match_count = match_count;

        
            args->start_idx = i * players_per_thread;
            args->end_idx = (i == NUM_THREADS - 1) ? player_count : args->start_idx + players_per_thread;
        

        printf("Player: %d - %d \n \n", args->start_idx, args->end_idx);

        pthread_create(&matches_producer_threads[i], NULL, main_producer, args);
    }

    for (int i = 0; i < NUM_THREADS; i++) 
    {
        ProducerArgs *args = malloc(sizeof(ProducerArgs));
        args->thread_id = i;
        args->data_type = Match;
        args->players = players;
        args->matches = matches;
        args->player_count = player_count;
        args->match_count = match_count;

        
            args->start_idx = i * matches_per_thread;
            args->end_idx = (i == NUM_THREADS - 1) ? match_count : args->start_idx + matches_per_thread;
        

        printf("Match: %d - %d \n \n", args->start_idx, args->end_idx);

        pthread_create(&players_producer_threads[i], NULL, main_producer, args);
    }

    pthread_mutex_lock(&producer_done_mutex);
    while (producers_done_count < NUM_THREADS * 2) 
    {
        pthread_cond_wait(&producer_done_cond, &producer_done_mutex);
    }
    pthread_mutex_unlock(&producer_done_mutex);

    player_stats_generator();

    for (int i = 0; i < NUM_THREADS; i++)
    {
        BUFFER_ARGS *args = malloc(sizeof(BUFFER_ARGS));
        args->thread_id = i;
        
            args->start_index = i * statistics_per_thread;
            args->end_index = (i == NUM_THREADS - 1) ? player_count : args->start_index + statistics_per_thread;
        

        printf("Player: %d - %d \n \n", args->start_index, args->end_index);

        pthread_create(&statistics_producer_threads[i], NULL, buffer_producer, args);
    }

    pthread_mutex_lock(&buffer_done_mutex);
    while (buffer_insertions_count < NUM_THREADS) 
    {
        pthread_cond_wait(&buffer_done_cond, &buffer_done_mutex);
    }
    pthread_mutex_unlock(&buffer_done_mutex);

    for (int i = 0; i < NUM_THREADS; i++)
    {
        BUFFER_ARGS *args = malloc(sizeof(BUFFER_ARGS));
        args->thread_id = i;
        
            args->start_index = i * scores_calculated_per_thread;
            args->end_index = (i == NUM_THREADS - 1) ? player_count : args->start_index + scores_calculated_per_thread;
        

        printf("Player: %d - %d \n \n", args->start_index, args->end_index);

        pthread_create(&score_calculating_threads[i], NULL, score_calculating_thread_function, args);
    }

    pthread_mutex_lock(&score_done_mutex);
    while (score_insertions_count < NUM_THREADS) 
    {
        pthread_cond_wait(&score_done_cond, &score_done_mutex);
    }
    pthread_mutex_unlock(&score_done_mutex);

    BUFFER_ARGS *args = malloc(sizeof(BUFFER_ARGS));

    args->start_index = 0;
    args->end_index = real_number_of_players;

    pthread_t thread_id;

    pthread_create(&thread_id, NULL, quick_sort, args);

    // Wait for all producer threads to finish - matches
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(matches_producer_threads[i], NULL);
    }

    // Wait for all producer threads to finish - players_profiles
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(players_producer_threads[i], NULL);
    }

    // Wait for all producer threads to finish - buffer_statistics
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(statistics_producer_threads[i], NULL);
    }

    // Wait for all producer threads to finish - score caclulating and assigning to the elements from the buffer
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(score_calculating_threads[i], NULL);
    }

    pthread_join(thread_id, NULL);



    // DEBUG:
    for (int i = 0; i < real_number_of_players; i++)
    {
            printf("Player ID: %d, Name: %s %s, Assists: %d, Goals: %d, Own Goals: %d, Red Cards: %d, Yellow Cards: %d, Score: %d\n",
                   playerBuffer[i].playerId,
                   playerBuffer[i].firstName,
                   playerBuffer[i].lastName,
                   playerBuffer[i].assists,
                   playerBuffer[i].goals,
                   playerBuffer[i].ownGoals,
                   playerBuffer[i].redCards,
                   playerBuffer[i].yellowCards,
                   playerBuffer[i].score);
    }

    printf("\n\n\n EXCLUSIV\n TOPUL celor 10 jucatori este: \n\n");

    int count = 1;

    for (int i = real_number_of_players-1; i >= real_number_of_players - 10; i--)
    {
            printf("LOCUL: %d -> Player ID: %d, Name: %s %s, Assists: %d, Goals: %d, Own Goals: %d, Red Cards: %d, Yellow Cards: %d, Score: %d\n",
                   count++,
                   playerBuffer[i].playerId,
                   playerBuffer[i].firstName,
                   playerBuffer[i].lastName,
                   playerBuffer[i].assists,
                   playerBuffer[i].goals,
                   playerBuffer[i].ownGoals,
                   playerBuffer[i].redCards,
                   playerBuffer[i].yellowCards,
                   playerBuffer[i].score);
    }



    stop_profiling = 1;
    pthread_join(profiler_thread, NULL);


    printf("Real number of players is : %d \n", real_number_of_players);


    
    pthread_mutex_destroy(&lock);
    pthread_cond_destroy(&empty_cond);

    pthread_mutex_destroy(&producer_done_mutex);
    pthread_cond_destroy(&producer_done_cond);

    pthread_mutex_destroy(&number_of_players_mutex); 

    pthread_mutex_destroy(&buffer_done_mutex);
    pthread_cond_destroy(&buffer_done_cond);

    pthread_mutex_destroy(&score_done_mutex);
    pthread_cond_destroy(&score_done_cond);

    return 0;
}