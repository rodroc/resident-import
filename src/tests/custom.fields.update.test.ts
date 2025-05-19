import ResidentParser from "../ResidentParser"
import * as routine from  "./routine"
import fs from 'fs'
import { ICustomFieldMetaData, IDbProfiles, IJsonUsers } from "../constants"
import {QueryTypes} from 'sequelize'
import {ICustomFieldMetaDatas} from '../constants'
import DbTask from '../DbTask'

const tagLabels={
    Units:{
        LeaseID: 'Lease ID'
    },
    Users:{
        Mailing: {
            Address1:'Mailing Address 1',
            Address2:'Mailing Address 2',
            City:'Mailing City',
            State: 'Mailing State',
            Postal: 'Mailing Postal',
            Country: 'Mailing Country'
        }
    }
}

interface IMailingAddress{
    unit_id:number,
    unit_title: string,
    tenant_id:number|string,
    mailing_address_1:string,
    mailing_address_2:string,
    mailing_city: string,
    mailing_state: string,
    mailing_postal: string,
    mailing_country: string
}

interface IMetaDataCustomField{
    id: number,
    id_entity:number,
    id_object: number|string,
    id_field: number,
    value: string,
    tag_label: string
}

const sqlMetaData=`SELECT md.id,md.id_entity,md.id_object,md.id_field,md.value,cf.tag_label 
FROM \`${routine.customerDb}\`.package__custom_fields_entity_metadata md
JOIN \`${routine.customerDb}\`.package__custom_fields_field cf 
ON cf.id=md.id_field AND cf.id_entity=md.id_entity 
WHERE cf.id_community=1`

const sqlProfiles=`SELECT p.id AS id_profile,p.id_user,r.username,r.email,p.is_deleted,p.id_account
FROM ${routine.customerDb}.package__profiler p
LEFT JOIN ${routine.instancesDb}.cp__role r ON r.id=p.id_user
;`

test(`Update meta data when the same tenant/resident changes address.`, async ()=>{

    const db = await routine.getSequelize()
    
    await routine.truncateCustomerTables()
    await routine.truncateServerTables()

    let parser = new ResidentParser(1)
    await parser.init()    

    // 1st file
    let JSONfile = './src/tests/import/custom.fields.update.mailing.address.file1.json'
    let jsonBuffer = fs.readFileSync(JSONfile)

    // inputs
    let mappedUsers = await parser.mapUnitUsersToCustomFields(jsonBuffer)
    let mUsers  = mappedUsers.slice() as Array<IMailingAddress>

    expect(mUsers.length).toBe(1)
    expect(mUsers[0]).toBeTruthy()
    expect(mUsers[0]).toHaveProperty('unit_id')

    expect(mUsers[0]['unit_id']).toBe(537)
    expect(mUsers[0]['unit_title']).toBe('537-D06')
    expect(mUsers[0]['tenant_id']).toBe(7895)
    expect(mUsers[0]['mailing_address_1']).toBe('old M.Address 1')
    expect(mUsers[0]['mailing_address_2']).toBe('old M.Address 2')
    expect(mUsers[0]['mailing_city']).toBe('old City')
    expect(mUsers[0]['mailing_state']).toBe('old State')
    expect(mUsers[0]['mailing_postal']).toBe('old Postal code')
    expect(mUsers[0]['mailing_country']).toBe('old M.Country')

    let parseResult = await parser.sync(jsonBuffer)
    expect(parseResult.success).toBe(true)

    let profiles = await db.query(sqlProfiles,{
        type: QueryTypes.SELECT
    }) as  IDbProfiles
    expect(profiles.length).toBe(1)

    expect(profiles[0].id_account).toBe('7895')

    let metaDatas = await db.query(sqlMetaData,{
            type: QueryTypes.SELECT
        }) as  Array<IMetaDataCustomField>
    let metaData: IMetaDataCustomField|undefined
    metaData=metaDatas.find(d=>d.tag_label==tagLabels.Users.Mailing.Address1)
    expect(metaData?.value).toBe('old M.Address 1')
    metaData=metaDatas.find(d=>d.tag_label==tagLabels.Users.Mailing.Address2)
    expect(metaData?.value).toBe('old M.Address 2')
    metaData=metaDatas.find(d=>d.tag_label==tagLabels.Users.Mailing.City)
    expect(metaData?.value).toBe('old City')
    metaData=metaDatas.find(d=>d.tag_label==tagLabels.Users.Mailing.State)
    expect(metaData?.value).toBe('old State')
    metaData=metaDatas.find(d=>d.tag_label==tagLabels.Users.Mailing.Postal)
    expect(metaData?.value).toBe('old Postal code')
    metaData=metaDatas.find(d=>d.tag_label==tagLabels.Users.Mailing.Country)
    expect(metaData?.value).toBe('old M.Country')

    // 2nd file
    JSONfile = './src/tests/import/custom.fields.update.mailing.address.file2.json'
    jsonBuffer = fs.readFileSync(JSONfile)

    parseResult = await parser.sync(jsonBuffer)
    expect(parseResult.success).toBe(true)

    profiles = await db.query(sqlProfiles,{
        type: QueryTypes.SELECT
    }) as  IDbProfiles
    expect(profiles.length).toBe(1)
    expect(profiles[0].id_account).toBe('7895')

    metaDatas = await db.query(sqlMetaData,{
        type: QueryTypes.SELECT
    }) as  Array<IMetaDataCustomField>
    metaData=metaDatas.find(d=>d.tag_label==tagLabels.Users.Mailing.Address1)
    expect(metaData?.value).toBe('new M.Address 1')
    metaData=metaDatas.find(d=>d.tag_label==tagLabels.Users.Mailing.Address2)
    expect(metaData?.value).toBe('new M.Address 2')
    metaData=metaDatas.find(d=>d.tag_label==tagLabels.Users.Mailing.City)
    expect(metaData?.value).toBe('new City')
    metaData=metaDatas.find(d=>d.tag_label==tagLabels.Users.Mailing.State)
    expect(metaData?.value).toBe('new State')
    metaData=metaDatas.find(d=>d.tag_label==tagLabels.Users.Mailing.Postal)
    expect(metaData?.value).toBe('new Postal code')
    metaData=metaDatas.find(d=>d.tag_label==tagLabels.Users.Mailing.Country)
    expect(metaData?.value).toBe('new M.Country')
    
})

test(`Replace leaseID of old tenant when new tenant takes over the same unit.`, async ()=>{
    const db = await routine.getSequelize()
    
    await routine.truncateCustomerTables()
    await routine.truncateServerTables()

    let parser = new ResidentParser(1)
    await parser.init()    

    // ! 1st file
    let JSONfile = './src/tests/import/custom.fields.update.b1.json'
    let jsonBuffer = fs.readFileSync(JSONfile)

    // inputs
    let mappedUsers = await parser.mapUnitUsersToCustomFields(jsonBuffer)
    let mUsers  = mappedUsers.slice() as Array<IMailingAddress>

    expect(mUsers.length).toBe(1)

    // input
    let oldTenant = mUsers.find(u=>u.tenant_id==7895)
    expect(oldTenant?.unit_id).toBe(537)
    expect(oldTenant?.unit_title).toBe('537-D06')
    expect(oldTenant?.mailing_address_1).toBe('Tom address')

    let parseResult = await parser.sync(jsonBuffer)
    expect(parseResult.success).toBe(true)

    let metaDatas = await db.query(sqlMetaData,{
        type: QueryTypes.SELECT
    }) as  Array<IMetaDataCustomField>

    // ! 2nd file

    JSONfile = './src/tests/import/custom.fields.update.b2.json'
    jsonBuffer = fs.readFileSync(JSONfile)

    mappedUsers = await parser.mapUnitUsersToCustomFields(jsonBuffer)
    mUsers  = mappedUsers.slice() as Array<IMailingAddress>

    expect(mUsers.length).toBe(1)

    // input
    const newTenant = mUsers.find(u=>u.tenant_id==9323)
    expect(newTenant?.unit_id).toBe(537) // same unit
    expect(newTenant?.unit_title).toBe('537-D06') // same unit
    expect(newTenant?.mailing_address_1).toBe('Jerry address')

    parseResult = await parser.sync(jsonBuffer)
    expect(parseResult.success).toBe(true)

    metaDatas = await db.query(sqlMetaData,{
        type: QueryTypes.SELECT
    }) as  Array<IMetaDataCustomField>

    const leaseIDs = metaDatas.filter(d=>d.tag_label==tagLabels.Units.LeaseID)
    expect(leaseIDs.length).toBe(1)
    expect(leaseIDs[0].value).toBe('537') 
    // unit should belong to new tenant
    // ! check unit profiles
    const unitProfiles = await db.query(`SELECT up.id_unit,u.title,p.id_user,p.email 
    FROM \`${routine.customerDb}\`.package__unit_manager_unit_profiles up
    JOIN  \`${routine.customerDb}\`.package__profiler p ON p.id_user=up.id_user
    JOIN  \`${routine.customerDb}\`.package__unit_manager_units u ON u.id=up.id_unit
    `,{
    type: QueryTypes.SELECT
    }) as  Array<{title:string,email:string}>
    expect(unitProfiles.length).toBe(1)
    expect(unitProfiles[0].title).toBe('537-D06')
    expect(unitProfiles[0].email).toBe('jerry@vagrant.test')

})