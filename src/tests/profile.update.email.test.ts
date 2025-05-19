import ResidentParser from "../ResidentParser"
import * as routine from  "./routine"
import fs from 'fs'
import {QueryTypes} from 'sequelize'
import {TDbUserProfile,IDbUserProfiles,TDbProfile,IDbProfiles} from '../constants'

const userProfilesSql = `SELECT r.id,r.username,r.email AS user_email,r.display_name AS user_display_name,p.id_account,r.email AS profile_email,r.display_name AS profile_display_name,r.is_valid_email,p.is_deleted,p.is_acl_actived
    FROM \`${routine.instancesDb}\`.cp__role r
    LEFT JOIN \`${routine.customerDb}\`.package__profiler p ON p.id_user=r.id
    ORDER BY r.id ASC;
    `

const profilesSql = `SELECT id,id_user,first_name,last_name,display_name,email,id_account,is_deleted,is_local,is_acl_actived
            FROM \`${routine.customerDb}\`.package__profiler`

test(`Disallow setting empty email when existing profile w/ valid email matches the import`, async ()=>{

    const db = await routine.getSequelize()
    
    await routine.truncateCustomerTables()
    await routine.truncateServerTables()

    let parser = new ResidentParser(1)
    await parser.init()    

    // ! import file # 1
    let JSONfile = './src/tests/import/profile.update.email.1.json'
    let jsonBuffer = fs.readFileSync(JSONfile)

    let mappedUsers = await parser.mapUnitUsersToCustomFields(jsonBuffer)
    let mUsers  = mappedUsers.slice() as Array<{unit_title:string,email:string}>

    expect(mUsers.length).toBe(1)
    expect(mUsers[0]).toBeTruthy()
    // no email
    expect(mUsers[0]['email']).toBe('')

    let parseResult = await parser.sync(jsonBuffer)
    expect(parseResult.success).toBe(true)

    let userProfiles = await db.query(
    userProfilesSql,{
        type: QueryTypes.SELECT
    }) as  IDbUserProfiles
    expect(userProfiles.length).toBe(1)
    expect(userProfiles[0].user_email).toBe(null)
    expect(userProfiles[0].profile_email).toBe(null)

    let profiles = await db.query(
    profilesSql,{
        type: QueryTypes.SELECT
    }) as  IDbProfiles
    expect(profiles.length).toBe(1)
    expect(profiles[0].email).toBe(null)
    expect(profiles[0].is_local).toBe(1) 
    expect(profiles[0].is_acl_actived).toBe(1)
    expect(profiles[0].id_user).toBe(1) // new user w/out email

    // ! import file # 2
    JSONfile = './src/tests/import/profile.update.email.2.json'
    jsonBuffer = fs.readFileSync(JSONfile)

    mappedUsers = await parser.mapUnitUsersToCustomFields(jsonBuffer)
    mUsers  = mappedUsers.slice() as Array<{unit_title:string,email:string}>

    expect(mUsers.length).toBe(1)
    expect(mUsers[0]).toBeTruthy()
    // email changed from blank to a valid email
    expect(mUsers[0]['email']).toBe('jerry@vagrant.test')

    parseResult = await parser.sync(jsonBuffer)
    expect(parseResult.success).toBe(true)
    // return

    userProfiles = await db.query(
        userProfilesSql,{
            type: QueryTypes.SELECT
        }) as  IDbUserProfiles
    expect(userProfiles.length).toBe(2)
    expect(userProfiles[1].user_email).toBe('jerry@vagrant.test')
    expect(userProfiles[1].profile_email).toBe('jerry@vagrant.test')

    let profileWithEmail = userProfiles.find(f=>f.user_email=='jerry@vagrant.test') as TDbUserProfile
    expect(profileWithEmail).toBeDefined()
    // return

    profiles = await db.query(
    profilesSql,{
        type: QueryTypes.SELECT
    }) as  IDbProfiles
    expect(profiles.length).toBe(1)
    expect(profiles[0].email).toBe('jerry@vagrant.test')
    expect(profiles[0].is_local).toBe(0) 
    expect(profiles[0].is_acl_actived).toBe(1)
    expect(profiles[0].id_user).toBe(2) // new user w/ email
    // return

    // ! import file # 3
    JSONfile = './src/tests/import/profile.update.email.3.json'
    jsonBuffer = fs.readFileSync(JSONfile)

    mappedUsers = await parser.mapUnitUsersToCustomFields(jsonBuffer)
    mUsers  = mappedUsers.slice() as Array<{unit_title:string,email:string}>

    expect(mUsers.length).toBe(1)
    expect(mUsers[0]).toBeTruthy()
    // email changed from jerry to jerry2
    expect(mUsers[0]['email']).toBe('jerry2@vagrant.test')

    parseResult = await parser.sync(jsonBuffer)
    expect(parseResult.success).toBe(true)

    userProfiles = await db.query(
        userProfilesSql,{
            type: QueryTypes.SELECT
        }) as  IDbUserProfiles
    expect(userProfiles.length).toBe(3)
    expect(userProfiles[2].user_email).toBe('jerry2@vagrant.test')
    expect(userProfiles[2].profile_email).toBe('jerry2@vagrant.test')

    profileWithEmail=userProfiles.find(d=>d.user_email=='jerry2@vagrant.test') as TDbUserProfile
    expect(profileWithEmail).toBeDefined()

    profiles = await db.query(
    profilesSql,{
        type: QueryTypes.SELECT
    }) as  IDbProfiles
    expect(profiles.length).toBe(1)
    expect(profiles[0].email).toBe('jerry2@vagrant.test')
    expect(profiles[0].is_local).toBe(0) 
    expect(profiles[0].is_acl_actived).toBe(1)
    expect(profiles[0].id_user).toBe(3) // user w/ jerry2
    // return

    // ! import file # 4
    JSONfile = './src/tests/import/profile.update.email.4.json'
    jsonBuffer = fs.readFileSync(JSONfile)

    mappedUsers = await parser.mapUnitUsersToCustomFields(jsonBuffer)
    mUsers  = mappedUsers.slice() as Array<{unit_title:string,email:string}>

    expect(mUsers.length).toBe(1)
    expect(mUsers[0]).toBeTruthy()
    // email changed from jerry2 to blank/empty
    expect(mUsers[0]['email']).toBe('')

    parseResult = await parser.sync(jsonBuffer)
    expect(parseResult.success).toBe(true)

    userProfiles = await db.query(
        userProfilesSql,{
            type: QueryTypes.SELECT
        }) as  IDbUserProfiles
    expect(userProfiles.length).toBe(3)

    profiles = await db.query(
    profilesSql,{
        type: QueryTypes.SELECT
    }) as  IDbProfiles
    expect(profiles.length).toBe(1)
    // ! obsolete: email should not be replaced by blank/empty value
    // ! obsolete: expect(profiles[0].email).toBe('jerry2@vagrant.test')
    expect(profiles[0].email).toBeNull()
    expect(profiles[0].is_local).toBe(1) 
    expect(profiles[0].is_acl_actived).toBe(1)
    expect(profiles[0].id_user).toBe(1)
})