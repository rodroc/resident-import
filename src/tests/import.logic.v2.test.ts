import ResidentParser from "../ResidentParser"
import  {extractEmails} from "../Helper"
import * as routine from  "./routine"
import fs from 'fs'
import {QueryTypes} from 'sequelize'
import { ExceptionHandler } from "winston"
import { IJsonUser, IJsonUsers } from "../constants"
import _,{map} from 'lodash'

test(`Import Logic v2:Case 1a: Field requirements`, async()=>{

    const db = await routine.getSequelize()
    
    await routine.truncateCustomerTables()
    await routine.truncateServerTables()

    const parser = new ResidentParser(1)
    await parser.init()

    const JSONfile = './src/tests/import/import.logic.v2.case01a.format2.json'
    const jsonBuffer = fs.readFileSync(JSONfile)

    let mappedUsers = await parser.mapUnitUsersToCustomFields(jsonBuffer)
    let jUsers  = mappedUsers.slice() as IJsonUsers

    expect(jUsers.length).toBe(3)    
    
    expect(jUsers[0].tenant_id).toBe(100)
    expect(jUsers[0].email).toBe('')

    expect(jUsers[1].tenant_id).toBe(111)
    expect(jUsers[1].email).toBe('')

    expect(jUsers[2].tenant_id).toBe(222)
    expect(jUsers[2].email).toBe('johnsmith@test.com')

    const emails = [...new Set(jUsers.filter(f=>f.email).map(m=>m.email))]
    expect(emails.length).toBe(1)
    
    const parseResult = await parser.sync(jsonBuffer)
    expect(parseResult.success).toBe(true)

    const dbUsers = await db.query(`SELECT id,email,display_name FROM \`${routine.customerDb}\`.
    package__profiler;`,{
        type: QueryTypes.SELECT
    }) as IUnits
    expect(dbUsers).toBeInstanceOf(Object)
    expect(dbUsers.length).toBe(3)

    // input: unit_title
    expect(jUsers[0].unit_title).toHaveLength(0)        
    expect(jUsers[1].unit_title).toBe('buildnum-unitno')
    expect(jUsers[2].unit_title).toBe('buildnum-unitno2')

    interface IUnit{
        id:number,title:string
    }
    interface IUnits extends Array<IUnit>{}

    const dbUnits = await db.query(`SELECT id,title FROM \`${routine.customerDb}\`.
    package__unit_manager_units;`,{
        type: QueryTypes.SELECT
    }) as IUnits
    expect(dbUnits).toBeInstanceOf(Object)
    expect(dbUnits.length).toBe(2)

    const unitList = dbUnits.map(m=>{
        return { id:m.id,title:m.title }
    })

    // input: unit title to be empty string
    let unitTitle=''    
    expect(jUsers[0].unit_title).toBe('')

    let foundUnit:IUnit|undefined = unitList.find(u=>u.title==unitTitle)
    // if there's no unit_title,it should not be imported
    expect(foundUnit).toBeUndefined()

    // input: building number has value
    unitTitle='buildnum-unitno'
    expect(jUsers[1].unit_title).toBe(unitTitle)

    foundUnit = unitList.find(u=>u.title && u.title.toString()==unitTitle)
    // unit title should  be in db
    expect(foundUnit).toBeDefined()

    unitTitle='buildnum-unitno2'
    expect(jUsers[2].unit_title).toBe(unitTitle)

    foundUnit = unitList.find(u=>u.title && u.title.toString()==unitTitle)
    // unit title should be in db
    expect(foundUnit).toBeDefined()

    // check if mapped array still has values
     expect(unitList.length).toBe(2)
})

test(`Import Logic v2:Case 1b: If a unique ID is associated with same unit with different Last Names, First name,but same Email.`,async()=>{

    const db = await routine.getSequelize()
    
    await routine.truncateCustomerTables()
    await routine.truncateServerTables()

    const parser = new ResidentParser(1)
    await parser.init()

    const JSONfile = './src/tests/import/import.logic.v2.case01b.format2.json'
    const jsonBuffer = fs.readFileSync(JSONfile)
    let mappedUsers = await parser.mapUnitUsersToCustomFields(jsonBuffer)
    let jUsers  = mappedUsers.slice() as IJsonUsers    

    // inputs

     // 2 rows from the text file
    expect(jUsers.length).toBe(2)

    // expect(jUsers[0].tenant_id).toBe('100')
    expect(jUsers[0].first_name).toBe('ASF Carroll')
    expect(jUsers[0].last_name).toBe('LLC')
    expect(jUsers[0].email).toBe('jdfake@gmail.com')

    //expect(jUsers[1].tenant_id).toBe('111')
    expect(jUsers[1].first_name).toBe('JD')
    expect(jUsers[1].last_name).toBe('Fake')
    expect(jUsers[1].email).toBe('jdfake@gmail.com')

    const emails = [...new Set(jUsers.filter(f=>f.email).map(m=>m.email))]
    // there should be 1 unique email
    expect(emails.length).toBe(1)

    const emailList =  emails.map(m=>`'${m}'`).join(',')

    const parseResult = await parser.sync(jsonBuffer)
    expect(parseResult.success).toBe(true)
    
    let sql=`SELECT p.id,p.first_name,p.last_name,p.email,cr.username
    FROM ${routine.customerDb}.package__profiler p
    JOIN ${routine.instancesDb}.cp__role cr ON cr.id=p.id_user
    WHERE   p.email IN (${emailList})
        AND cr.email IN (${emailList})
        AND cr.username IN (${emailList})
        AND p.is_deleted=0
    ;`
    const userProfiles = await db.query(sql,{
        type: QueryTypes.SELECT
    }) as Array<{first_name:string,last_name:string}>
    // only 1 profile should get pulled since the other one gets soft deleted
    expect(userProfiles.length).toBe(1)
    expect(userProfiles[0].first_name).toBe('JD')
    expect(userProfiles[0].last_name).toBe('Fake')
})

test(`Import Logic v2:Case 2: If  two or more units are associated  with the same first name, Lastname, and email`,async()=>{

    const db = await routine.getSequelize()

    await routine.truncateCustomerTables()
    await routine.truncateServerTables()

    const parser = new ResidentParser(1)
    await parser.init()

    const JSONfile = './src/tests/import/import.logic.v2.case02.format2.json'
    const jsonBuffer = fs.readFileSync(JSONfile)

    let mappedUsers = await parser.mapUnitUsersToCustomFields(jsonBuffer)
    let jUsers  = mappedUsers.slice() as IJsonUsers    

    // inputs
    expect(jUsers.length).toBe(3)

    expect(jUsers[0].first_name).toBe('ASF Carroll')
    expect(jUsers[0].last_name).toBe('LLC')
    expect(jUsers[0].email).toBe('jdfake@gmail.com')
    expect(jUsers[0].unit_title).toBe('170-A2')
    expect(jUsers[0].tenant_id).toBe(467)

    expect(jUsers[1].first_name).toBe('ASF Carroll')
    expect(jUsers[1].last_name).toBe('LLC')
    expect(jUsers[1].email).toBe('jdfake@gmail.com')
    expect(jUsers[1].unit_title).toBe('170-A3')
    expect(jUsers[1].tenant_id).toBe(469)

    expect(jUsers[2].first_name).toBe('ASF Carroll')
    expect(jUsers[2].last_name).toBe('LLC')
    expect(jUsers[2].email).toBe('jdfake@gmail.com')
    expect(jUsers[2].unit_title).toBe('170-A4')
    expect(jUsers[2].tenant_id).toBe(471)

    const emails = [...new Set(jUsers.filter(f=>f.email).map(m=>m.email))]
    // there should be 1 unique email
    expect(emails.length).toBe(1)

    const emailList =  emails.map(m=>`'${m}'`).join(',')

    const parseResult = await parser.sync(jsonBuffer)
    expect(parseResult.success).toBe(true)

    let sql=`SELECT p.id,p.first_name,p.last_name,p.email,cr.username,p.id_account
    FROM ${routine.customerDb}.package__profiler p
    JOIN ${routine.instancesDb}.cp__role cr ON cr.id=p.id_user
    WHERE   p.email IN (${emailList})
        AND cr.email IN (${emailList})
        AND cr.username IN (${emailList})
        AND p.is_deleted=0
    ;`
    const userProfiles = await db.query(sql,{
        type: QueryTypes.SELECT
    }) as Array<{first_name:string,last_name:string,id_account:string}>
    // only 1 profile should get pulled since the other one gets soft deleted
    expect(userProfiles.length).toBe(1)
    expect(userProfiles[0].first_name).toBe('Asf Carroll') // name formatted
    expect(userProfiles[0].last_name).toBe('LLC')
    expect(userProfiles[0].id_account).toBe('471')
})

test(`Import Logic v2:Case 3: If the same unit is associated with a different unique id, then with no FirstName, different LastName, and one has email.`,async()=>{

    const db = await routine.getSequelize()

    await routine.truncateCustomerTables()
    await routine.truncateServerTables()

    const parser = new ResidentParser(1)
    await parser.init()

    const JSONfile = './src/tests/import/import.logic.v2.case03.format2.json'
    const jsonBuffer = fs.readFileSync(JSONfile)
    let mappedUsers = await parser.mapUnitUsersToCustomFields(jsonBuffer)
    let jUsers  = mappedUsers.slice() as IJsonUsers

    // inputs
    expect(jUsers.length).toBe(2)

    expect(jUsers[0].first_name).toBe('Alain')
    expect(jUsers[0].last_name).toBe('')
    expect(jUsers[0].email).toBe('alainmercado@yahoo.com')
    expect(jUsers[0].unit_title).toBe('116-2E')
    expect(jUsers[0].tenant_id).toBe(406)

    expect(jUsers[1].first_name).toBe('Mercado')
    expect(jUsers[1].last_name).toBe('')
    expect(jUsers[1].email).toBe('')
    expect(jUsers[1].unit_title).toBe('116-2E')
    expect(jUsers[1].tenant_id).toBe(3493)

    // only 1 unique email expected
    const emails = [...new Set(jUsers.filter(f=>f.email).map(m=>m.email))]
    // there should be 1 unique email
    expect(emails.length).toBe(1)
    expect(emails[0]).toBe('alainmercado@yahoo.com')

    const emailList =  emails.map(m=>`'${m}'`).join(',')

    const parseResult = await parser.sync(jsonBuffer)
    expect(parseResult.success).toBe(true)

    let sql=`SELECT p.id,p.first_name,p.last_name,p.email,r.username,p.is_local
    FROM ${routine.customerDb}.package__profiler p
    JOIN ${routine.instancesDb}.cp__role r ON r.id=p.id_user
    ;`
    const userProfiles = await db.query(sql,{
        type: QueryTypes.SELECT
    }) as Array<{first_name:string,email:string,is_local:number}>
    expect(userProfiles.length).toBe(2)

    expect(userProfiles[0].first_name).toBe('Alain')
    expect(userProfiles[0].email).toBe('alainmercado@yahoo.com')
    expect(userProfiles[0].is_local).toBe(0)

    expect(userProfiles[1].first_name).toBe('Mercado')
    expect(userProfiles[1].email).toBeNull()
    // should hav is_local=1 when there's no email address    
    expect(userProfiles[1].is_local).toBe(1)
})

test(`Import Logic v2:Case 4: If one or more units are associated with a different unique ID, with the same FirstName,  LastName, and email.`, async()=>{

    const db = await routine.getSequelize()

    await routine.truncateCustomerTables()
    await routine.truncateServerTables()

    const parser = new ResidentParser(1)
    await parser.init()

    const JSONfile = './src/tests/import/import.logic.v2.case04.format2.json'
    const jsonBuffer = fs.readFileSync(JSONfile)

    let mappedUsers = await parser.mapUnitUsersToCustomFields(jsonBuffer)
    let jUsers  = mappedUsers.slice() as IJsonUsers

    // inputs
    expect(jUsers.length).toBe(5)

    const sameNames = jUsers.filter(u=>u.first_name && u.first_name=='1608 Realty Assoc.' && u.last_name =='LLC' )
    // all use the same name
    expect(sameNames.length).toBe(5)

    const sameEmail = jUsers.filter(u=>u.email && u.email=='mgoldstein15@gmail.com')
    // all use the same email
    expect(sameEmail.length).toBe(5)

    expect(jUsers[0].unit_title).toBe('160-6A')
    expect(jUsers[0].tenant_id.toString()).toBe('998')

    expect(jUsers[1].unit_title).toBe('160-6B')
    expect(jUsers[1].tenant_id.toString()).toBe('1000')

    expect(jUsers[2].unit_title).toBe('160-6D')
    expect(jUsers[2].tenant_id.toString()).toBe('1040')

    expect(jUsers[3].unit_title).toBe('160-6E')
    expect(jUsers[3].tenant_id.toString()).toBe('1004')

    expect(jUsers[4].unit_title).toBe('160-7A')
    expect(jUsers[4].tenant_id.toString()).toBe('1006')
    
    const emails = [...new Set(jUsers.filter(f=>f.email).map(m=>m.email))]
    // there should be 1 unique email
    expect(emails.length).toBe(1)

    const emailList =  emails.map(m=>`'${m}'`).join(',')

    const parseResult = await parser.sync(jsonBuffer)
    expect(parseResult.success).toBe(true)

    let sql=`SELECT p.id,p.first_name,p.last_name,p.email,r.username,p.is_deleted
    FROM ${routine.customerDb}.package__profiler p
    JOIN ${routine.instancesDb}.cp__role r ON r.id=p.id_user
    WHERE   p.email IN (${emailList})
        AND r.email IN (${emailList})
        AND r.username IN (${emailList})
    ;`
    const dbUserProfiles = await db.query(sql,{
        type: QueryTypes.SELECT
    }) as Array<{id:number,is_deleted:number}>

    // 5 profiles expected
    expect(dbUserProfiles.length).toBe(5)
    // 1 active
    expect( (dbUserProfiles.filter(f=>f.is_deleted==0).length) ).toBe(1)
    // 4 soft deleted
    expect( (dbUserProfiles.filter(f=>f.is_deleted==1).length) ).toBe(4)

    sql=`SELECT id,title
    FROM ${routine.customerDb}.package__unit_manager_units;`
    const dbUnits = await db.query(sql,{
        type: QueryTypes.SELECT
    })as Array<{id:number,title:string}>
    // 5 unique units expected
     expect(dbUnits.length).toBe(5)

    const unitTitles = dbUnits.map(u=>u.title)
    
    for await(const unitTitle of unitTitles){
        // each unit should belong to the same email
        sql=`SELECT up.id_user,cr.email,u.title 
        FROM ${routine.customerDb}.package__unit_manager_unit_profiles up
        JOIN ${routine.customerDb}.package__unit_manager_units u ON u.id=up.id_unit
        JOIN ${routine.instancesDb}.cp__role cr ON cr.id=up.id_user
        WHERE u.title='${unitTitle}' 
            AND cr.email=$email;`
        const dbUnitProfiles = await db.query(sql,{
            bind: {email:emails[0]},
            type: QueryTypes.SELECT
        })
        // only 1 row should get pulled for each unit for the same email
        expect(dbUnitProfiles.length).toBe(1)
    } // * for-of

})

test(`Import Logic v2:Case 5: If a unique id is associated with a same unit, with the same FirstName,  LastName, but different email.`, async()=>{

    const db = await routine.getSequelize()

    await routine.truncateCustomerTables()
    await routine.truncateServerTables()

    const parser = new ResidentParser(1)
    await parser.init()

    const JSONfile = './src/tests/import/import.logic.v2.case05.format2.json'
    const jsonBuffer = fs.readFileSync(JSONfile)

    let mappedUsers = await parser.mapUnitUsersToCustomFields(jsonBuffer)
    let jUsers  = mappedUsers.slice() as IJsonUsers

    // inputs
    expect(jUsers.length).toBe(4)

    // old user
    const sameUnitTitle='100-4C'
    const oldEmail='claudecline@outlook.com'
    const sameJAccounts=jUsers.filter(u=>u.email==oldEmail)
    for await(const j of sameJAccounts){
        expect(j.first_name).toBe('LLC')
        expect(j.last_name).toBe('Catz Partners')
        expect(j.email).toBe(oldEmail)
        expect(j.unit_title).toBe(sameUnitTitle)
        expect(j.tenant_id.toString()).toBe('127')
    }

    // latest user
    const latestEmail='claudecln@optonline.net'
    const lastestUser = jUsers.find(u=>u.email==latestEmail) as IJsonUser
    expect(lastestUser.first_name).toBe('LLC')
    expect(lastestUser.last_name).toBe('Catz Partners')
    expect(lastestUser.email).toBe(latestEmail)
    expect(lastestUser.unit_title).toBe('100-4C')
    expect(lastestUser.tenant_id.toString()).toBe("127")

    const emails=[...new Set(jUsers.filter(u=>u.email).map(m=>m.email as string))]
    // only 2 unique email expected
    expect(emails.length).toBe(2)
    const emailList = emails.map(m=>`'${m}'`).join(',')

    const jUnits = jUsers.filter(f=>f.unit_title).map(m=>m.unit_title as string)
    expect(jUnits.length).toBe(4)

    const uniqJUnits = [...new Set(jUnits)]
    // 1 unique unit expected from text file
    expect(uniqJUnits.length).toBe(1)

    const parseResult = await parser.sync(jsonBuffer)
    expect(parseResult.success).toBe(true)

    let sql=`SELECT p.id,p.first_name,p.last_name,p.email,r.username
    FROM ${routine.customerDb}.package__profiler p
    JOIN ${routine.instancesDb}.cp__role r ON r.id=p.id_user
    WHERE   p.email IN (${emailList})
        AND r.email IN (${emailList})
        AND r.username IN (${emailList})
        AND p.is_deleted=0
    ;`
    const userProfiles = await db.query(sql,{
        type: QueryTypes.SELECT
    }) as Array<{id:number}>
    

    // ! unit should belong to the latest email
    sql=`SELECT up.id_user,cr.email,u.title 
    FROM ${routine.customerDb}.package__unit_manager_unit_profiles up
    JOIN ${routine.customerDb}.package__unit_manager_units u ON u.id=up.id_unit
    JOIN ${routine.instancesDb}.cp__role cr ON cr.id=up.id_user
    WHERE u.title='${sameUnitTitle}' 
        AND cr.email IN (${emailList});`
    const unitProfiles = await db.query(sql,{
        type: QueryTypes.SELECT
    }) as Array<{id_user:number,email:string,title:string}>
    expect(unitProfiles.length).toBe(1)
    expect(unitProfiles[0].email).toBe(latestEmail)
})