"""
File name:              harvestFileRetriever.py
Author:                 Gabe VanSolkema
Python Version:         2.7

Date Created:           10 January 2017
Date Last Modified:     7 February 2017
Program Version:        1.0

Possible TODO:
    1) Add try/except blocks for error handling.
    2) Clean up code here and there.
"""

import json
import requests
import grequests
import datetime
import time
import os
import glob
import platform

""""
- User class with important, editable info that will be used in main function.

NOTE:
    (1) Username and password must be admin level for authentication purposes.
    (2) numDays: Number of time entries retrieved for each user.
    (3) throttleTime: Seconds between each user's asynchronous request.

EXTRA NOTE:
    (1) Increasing throttleTime makes program slower but helps to avoid
        throttling issues on Harvest's end.
    (2) Follow this naming convention for declaring pathname:
        "C:\Users\gabej_000\Desktop\Coding\Python\JsonFiles/"
    (3) Remember to include the final forward slash after pathname specified.
"""
class User(object):
    def __init__(self):
        self.username = "[REMOVED]"
        self.password = "[REMOVED]"
        self.numDays = 60
        self.throttleTime = 10
        self.individualEntryPath = "C:\HarvestImport\SplitJSON/"
        self.combinedEntryPath = "C:\HarvestImport/" #timeEntries.json
        self.usersPath = "C:\HarvestImport/" #userData.json
        self.projectsPath = "C:\HarvestImport/" #projectData.json
        self.tasksPath = "C:\HarvestImport/" #taskData.json
        self.clientsPath = "C:\HarvestImport/" #clientsData.json




"""
    MAIN FUNCTION TO BE CALLED WHEN FILE IS RAN.
"""
def main():
    """
        In a nutshell, the main function...

        (1) Holds most important data. The following are defined and used:
                - username (must be admin-level)
                - password (must be admin-level)
                - baseUrl
                - headers (JSON)
                - numDays
                - userID[]
                - throttleTime
                - year
                - day
                - getUsersUrl
                - userResponse
                - userData
                - gRequests[]
                - responses[]

        (2) Makes HTTPS request to retrieve all users using getUsersUrl.
                a) userResponse stores the response from the request.
                b) userData converts userResponse to JSON format.
                c) userData writes to 'userData.json'.

        (3) Reads 'userData.json'. Calculates numUsers and assigns userID.
                a) userData reads 'userData.json' and gets value.
                b) numUsers iterates through userData and adds users.
                c) userID[] appends ID value for each user from userData.

        (4) Stores URLs in groups of 5 in gRequests[].
                a) numDays specifies how far back requests go.
                b) userID[], day, year, & numDays are used to create URLs.
                c) URLs stored in gRequests[].
                d) Stores URLs for previous year if necessary
                e) Leap years adjust URL request to 366 days as opposed to 365.

        (5) Uses grequests module to make asynchronous HTTPS requests.
                a) HTTPS requests are made simultaneously for request[0].
                b) Throttle kicks in forcing program to wait.
                c) HTTPS requests are made simultaneously for request[1].
                d) Throttle kicks in forcing program to wait.
                e) HTTPS requests are made simultaneously for request[2].
                f) And so on...
                g) grequests MUST BE THROTTLED
                h) MUCH faster than making individual HTTPS requests.
                i) responses[] stores results of grequests
                j) throttleTime dictates the length of the throttle.

        (6) Writes grequest responses to file.
                a) Iterates through users and then responses for each user.
                a) Only writes responses when response status_code = 200
                b) Files named in the format: {userID}_{day}_{year}
                c) File naming accounts for leap year days as well.
                d) Old files are overwritten.

        (7) OPTIONAL: Rerun steps #4-6 above to double check initial grequests.
                a) Code is currently commented out.

        * All HTTPS requests are made using the following:
                - username
                - passworld
                - headers
                - baseUrl + {specific request info here}
    """

    print "\n-----------------------"
    print "Main function called..."
    print "-----------------------"

    """
    - Key variables defined at the top of file in the User Class.
    - Main function assigns User class variables to its own local variables.
    - Must have admin level username and password for authentication.
    - numDays: Number of time entries retrieved for each user.
    - throttleTime: Seconds between each user's asynchronous request.
    """
    user = User()
    username = user.username
    password = user.password
    numDays = user.numDays
    throttleTime = user.throttleTime


    # Used in HTTPS requests.
    # Base Url for get requests.
    baseUrl = 'https://tbdsolutions.harvestapp.com/'


    # Used in HTTPS requests.
    # Headers to specify get request data type.
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }


    # Number of users (will be calculated later).
    numUsers = 0


    # User ID list.
    userID = []


    # Year.
    year = datetime.datetime.now().year
    print "year: " + str(year)


    # Day.
    day = datetime.datetime.now().timetuple().tm_yday
    print "day: " + str(day)


    # URLs used for HTTPS get requests.
    getUsersUrl = baseUrl + "people"
    getProjectsUrl = baseUrl + "projects"
    getTasksUrl = baseUrl + "tasks"
    getClientsUrl = baseUrl + "clients"


    """ HERE WE BEGIN CALLING FUNCTIONS DEFINED BELOW MAIN FUNCTION"""
    # UserResponse for HTTPS get request.
    # Function called is defined below main.
    userResponse = getUrl(getUsersUrl, headers, username, password)


    # Set userData to userResponse in JSON format.
    userData = userResponse.json()


    """ Creates directories if they do not already exist """
    if (platform.system() == "Windows"):
        if not os.path.exists(user.usersPath):
            os.makedirs(str(user.usersPath))
        if not os.path.exists(user.projectsPath):
            os.makedirs(str(user.projectsPath))
        if not os.path.exists(user.tasksPath):
            os.makedirs(str(user.tasksPath))
        if not os.path.exists(user.clientsPath):
            os.makedirs(str(user.clientsPath))
        if not os.path.exists(user.combinedEntryPath):
            os.makedirs(str(user.combinedEntryPath))
        if not os.path.exists(user.individualEntryPath):
            os.makedirs(str(user.individualEntryPath))
    else:
        if not os.path.exists(user.usersPath):
            os.system('mkdir ' + str(user.usersPath))
        if not os.path.exists(user.projectsPath):
            os.system('mkdir ' + str(user.projectsPath))
        if not os.path.exists(user.tasksPath):
            os.system('mkdir ' + str(user.tasksPath))
        if not os.path.exists(user.clientsPath):
            os.system('mkdir ' + str(user.clientsPath))
        if not os.path.exists(user.combinedEntryPath):
            os.system('mkdir ' + str(user.combinedEntryPath))
        if not os.path.exists(user.individualEntryPath):
            os.system('mkdir ' + str(user.individualEntryPath))


    # Dump userData to filename 'userData.json' at specified path.
    with open(user.usersPath + 'userData.json', 'w') as outfile:
        json.dump(userData, outfile)


    # Read data from filename 'userData.json' at specified path.
    with open(user.usersPath + 'userData.json', 'r') as f:
        userData = json.load(f)


    # Get number of users from data.
    # Function called is defined below main.
    numUsers = getNumUsers(userData, numUsers)


    # Loop through users and assign IDs to userID list.
    # Function called is defined below main.
    userID = assignUserID(userData, numUsers, userID)


    # Requests made in sets of 5.
    # Number of requests per user calculated using numDays and numUsers
    numExtra = 0
    reqPerUser = 0
    if numDays % 5 == 0:
        reqPerUser = numDays / 5
    else:
        numExtra = numDays % 5
        reqPerUser = ((numDays - numExtra) / 5) + 1
    numRequests = reqPerUser * numUsers


    print "\n-----------------------"
    print "----Making Requests----"
    print "-----------------------"
    print "Number of users: " + str(numUsers)
    print "Number of requests pers user: " + str(reqPerUser)
    print "Number of total requests to be made: " + str(numRequests)


    # Empty gRequest list. Index for each user.
    gRequests = [None] * numRequests


    # Get URLs and store them in their respective gRequest list.
    # Function called is defined below main.
    x = 0
    for u in range(numUsers):
        curDay = 0
        for r in range(reqPerUser):
            if (r == reqPerUser - 1 and numExtra != 0):
                gRequests[x] = getUrls(u, userID, year, day, numDays, baseUrl,
                                username, password, headers, curDay, numExtra)
                x += 1
            else:
                gRequests[x] = getUrls(u, userID, year, day, numDays, baseUrl,
                                username, password, headers, curDay)
                curDay += 5
                x += 1


    # Empty response list. Index for each request.
    responses = [None] * numRequests

    #print len(gRequests)

    # Iterate through gRequest lists and gets the responses for each one.
    # Function called is defined below main.
    for r in range(len(gRequests)):
        responses[r] = getResponses(r, gRequests, userID, throttleTime)


    # Write initial responses to file
    # Function called is defined below main.
    writeToFile(user.individualEntryPath, responses, reqPerUser, numRequests, userID, day, year, numDays, curDay)


    """ Get projects JSON file. """
    projectsResponse = requests.get(getProjectsUrl, auth=(username, password),
                                        headers=headers)
    projectsResponse.json()
    with open(user.projectsPath + 'projectData.json', 'w') as out:
        out.write(projectsResponse.content)


    """ Get tasks JSON file. """
    tasksResponse = requests.get(getTasksUrl, auth=(username, password),
                                        headers= headers)
    tasksResponse.json()
    with open(user.tasksPath + 'taskData.json', 'w') as out:
        out.write(tasksResponse.content)

    """ Get clients JSON file. """
    clientsResponse = requests.get(getClientsUrl, auth=(username, password),
                                        headers=headers)
    clientsResponse.json()
    with open(user.clientsPath + 'clientData.json', 'w') as out:
        out.write(clientsResponse.content)


    """
    --OPTIONAL CODE TO DOUBLE CHECK THE INITIAL GREQUESTS BEING MADE ABOVE--

    **DEPRECATED**
    """

    #gRequestsCheck = [None] * numUsers
    #gRequestsCheck = getUrls(gRequestsCheck, userID, year, day, numDays,
    #                          baseUrl, username, password, headers)
    #responsesCheck = [None] * numUsers
    #responsesCheck = getResponses(responsesCheck, gRequestsCheck,
    #                              userID, throttleTime, backwards=True)
    #writeToFile(responsesCheck, userID, day, year, numDays, doubleCheck=True)


    """ Combine time entry JSON files """
    #read_files = glob.glob(user.individualEntryPath + "*entry.json")
    #with open(user.combinedEntryPath + 'combinedTimeEntry.json', "wb") as outfile:
    #    outfile.write('[{}]'.format('.'.join([open(f, "rb").read()
    #                                        for f in read_files])))

    #read2_files = glob.glob("*entry.json")
    #with open(user.combinedEntryPath + 'combinedTimeEntry2.json', "wb") as outfile:
    #    outfile.write('[{}]'.format(
    #                    ','.join([open(f, "rb").read() for f in read_files])))



"""
    FUNCTIONS.
"""
def getUrl(url, headers, username, password):
    """
        Makes an HTTPS request and returns response.
        Exits if response is invalid.
    """

    response = requests.get(url,
                            headers=headers,
                            auth=(username, password))

    if response.status_code != 200:
        print ('Status: ' + str(response.status_code) +
               'Problem with the request. Exiting.')
        exit()

    return response



def getNumUsers(userData, numUsers):
    """
        Gets number of users from userData.
    """

    for d in userData:
        numUsers += 1

    print "numUsers: " + str(numUsers)
    print "\n--------------------"
    print "------User IDs------"
    print "--------------------"

    return numUsers



def assignUserID(userData, numUsers, userID):
    """
        Loops through users and assign IDs to userID list.
    """

    for i in range(numUsers):
        userID.append(1)
        userID[i] = str(userData[i]['user']['id'])
        name = (userData[i]['user']['first_name'] + " " +
                userData[i]['user']['last_name'])
        print(userID[i] + "    \t" + name)

    return userID



def getUrls(user, userID, yr, day, numDays, baseUrl, username, password,
            header, curDay, numExtra=0):
    """
        Gets URLs and stores then in respective grequest lists.
    """

    #print "Getting URLs for user: " + userID[user]

    if (numExtra != 0):
        numDays = numExtra
    else:
        numDays = 5

    url = [None] * numDays

    if day >= numDays:
        for i in range(numDays):
            date = day - i - curDay
            year = yr
            if date <= 0:
                date += 366
                year -= 1
            extraUrl = "daily/" + str(date) + "/" + str(year)
            extraUrl += "?slim=1&of_user=" + str(userID[user])
            finalUrl = baseUrl + extraUrl
            url[i] = finalUrl
            #if user == 0:
                #print url[i]

        request = (grequests.get(u, headers=header,
                         auth=(username, password)) for u in url)
        return request

    else:
        extraDays = numDays - day
        urlIndex = 0
        lastYear = yr-1
        if (lastYear % 4) == 0:
            lastYearDay = 366
        else:
            lastYearDay = 365

        for i in range(day):
            extraUrl = "daily/" + str(day-i-curDay) + "/" + str(yr)
            extraUrl += "?slim=1&of_user=" + str(userID[user])
            finalUrl = baseUrl + extraUrl
            url[i] = finalUrl
            urlIndex += 1

            for e in range(extraDays):
                print "here"
                eDeq = lastYearDay - e
                extraUrl = "daily/" + str(eDay-curDay) + "/" + str(lastYear)
                extraUrl += "?slim=1&of_user=" + str(userID[user])
                finalUrl = baseUrl + extraUrl
                url[urlIndex] = finalUrl
                urlIndex += 1

            request = (grequests.get(u, headers=header,
                     auth=(username, password)) for u in url)
            return request

    print "URLs retrieved.\n"



def getResponses(r, requests, userID, throttle, backwards=False):
    """
        Iterates through response list and makes grequests.
        Uses time.sleep(throttle) to slow down grequests.
        Slowing grequests avoids throttling issues.
    """

    # Forwards (initial).
    if backwards == False:
        #print "---------------------------"
        #print "--Requesting Time Entries--"
        #print "---------------------------"
        #for r in range(len(userID)):
            #print "User: number: " + str(r)
            #if r > 0:
            #    print "Beginning throttle..."
            #    time.sleep(throttle)
            #    print "Throttle over."
            #print "\nRequesting time entries for user: " + str(userID[r])
        time.sleep(throttle)
        requestNum = r + 1
        print "Request number being made: " + str(requestNum)
        response = grequests.map(requests[r])
        return response

        #print "Time entries requested successfully."

    # Backwards (double check).
    else:
        print "---------------------------"
        print "--Double Checking Results--"
        print "---------------------------"
        z = len(userID) - 1
        #time.sleep(throttle)
        while z >= 0:
            if z < len(userID) - 1:
                time.sleep(throttle)
            print "Double check for user: " + str(userID[z])
            responses[z] = grequests.map(rs[z])
            z -= 1

    return responses




def doubleCheck(responses, rs, userID, results=False):
    """
        Double checks request responses.
        Retries request for a user if any of their response are invalid.
        Returns false if no invalid responses found.
    """

    redo = False
    for r in range(len(userID)):
        for response in responses[r]:
            if response.status_code != 200:
                redo = True
                print "Retrying request for: " + str(userID[r])
                responses[r] = grequests.map(rs[r])

    if results:
        return responses

    return redo




def writeToFile(individualEntryPath, responses, reqPerUser, numRequests, userID, day, year, numDays, curDay, doubleCheck=False):
    """
        Writes the responses to their respective files.
        Only writes responses where status_code = 200
        Overwrites old files.
        File naming convention is as follows: {userID}_{day}_{year}
    """

    allFilesPassed = True

    if doubleCheck == False:
        print "\nWriting initial files."
    else:
        print "Writing double check files"

    z = 0

    for n in range(numRequests):
        #x = 0
        #z = 0
        #user = 0
        name = None

        #if (day >= numDays):
        for response in responses[n]:
            #name = str(userID[user]) + "_" + str(day-x) + "_"
            #name += str(year) + ".json"
            name = str(z) + "entry.json"
            z += 1
            #x += 1

            if response is None:
                print "Response type: None"
            elif response.status_code == 200:
                with open(individualEntryPath + str(name), 'wb') as f:
                    f.write(response.content)
            else:
                print "Initial response write to file failed: " + name
                allFilesPassed = False

        """
        else:
            if (year-1 % 4) == 0:
                lastYearDay = 366
            else:
                lastYearDay = 365

            lastYear = year - 1

            for response in responses[n]:
                if (day > x):
                    name = str(userID[n]) + "_" + str(day-x) + "_"
                    name += str(year) + ".json"
                    x += 1
                else:
                    name = str(userID[n]) + "_" + str(lastYearDay+1-z)
                    name += "_" + str(lastYear)
                    z += 1

                if response is None:
                    print "Response type: None"
                elif response.status_code == 200:
                    with open(name, 'wb') as f:
                        f.write(response.content)

                else:
                    print "Check response write to file failed: " + name
                    allFilesPassed = False
        """

    print "All responses valid and all files written: "+str(allFilesPassed)+"\n"



"""
    CALL TO MAIN FUNCTION.
"""
if __name__ == "__main__":
    main()
