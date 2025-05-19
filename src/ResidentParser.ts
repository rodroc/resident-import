import appConfig from './shared/appConfig'
import { /*...interfaces/etc...*/
} from './constants'
import {ImportInstanceAlreadyRunning} from './Exceptions'
import DbTask  from './DbTask'
import { extractEmails, IPhoneFormat, parsePhone } from './Helper'
import {BinTree} from 'bintrees'
import _, { kebabCase, uniq, filter,map,has } from 'lodash'
import dayjs from 'dayjs'
import advancedFormat from 'dayjs/plugin/advancedFormat'
import { addEmitHelper, isAwaitExpression } from 'typescript'
import { off } from 'process'
import e from 'express'
import isEmail from 'validator/lib/isEmail'
import * as Formatting from './Formatting'
import * as aSync from 'async';

dayjs.extend(advancedFormat)

class ResidentParser{

    private readonly settings=appConfig.settings()
    private id_instance:number=0
    private instancesDb: string
    private instanceData: IInstanceData = {
        id: 0, id_community: 0, database: '',server_guid:''
    }
    private dbTask: DbTask

    private residentGroups: IResidentGroups = []
    private residentGroupNames: string[] = [
        ResidentGroup.Owners,
        ResidentGroup.Residents,
        ResidentGroup.Tenant
    ]

    constructor(id_instance: number){
        this.id_instance=id_instance
        this.instancesDb = this.settings.isProduction? this.settings.mysql.master.database.instances:this.settings.mysql.development.database.instances
        this.dbTask = new DbTask({
            id_instance
        })
    }

    public async init(){
        try{
            await this.dbTask.init(/*this.id_instance*/)
            this.instanceData = this.dbTask.getInstanceData()

            if( ! await this.dbTask.hasCustomFieldConfigFile() ){
                throw new Error(`Missing config file`)
            }

            let roleGroups = await this.dbTask.getUserGroupNames()
            if( roleGroups.length==0 ){
                throw new Error(`Missing groups: Residents, Tenant & Owners.`)
            }
            let owners:IUserGroupName|undefined,residents:IUserGroupName|undefined,tenant:IUserGroupName|undefined

            owners = await ( async () => {
                return roleGroups.find(g=>g.name.toUpperCase()==ResidentGroup.Owners.toUpperCase())
            })()
            if( owners ){
                this.residentGroups.push(owners as IResidentGroup)
            }
            residents = await ( async () => {
                return roleGroups.find(g=>g.name.toUpperCase()==ResidentGroup.Residents.toUpperCase())
            })()
            if( residents ){
                this.residentGroups.push(residents as IResidentGroup)
            }
            tenant = await ( async () => {
                return roleGroups.find(g=>g.name.toUpperCase()==ResidentGroup.Tenant.toUpperCase())
            })()
            if( tenant ){
                this.residentGroups.push(tenant as IResidentGroup)
            }
            if( ( !owners || !residents || !tenant) || this.residentGroups.length!=3 ) {
                throw new Error(`All user groups: Residents, Tenant & Owners must exists. Please contact the dev/administrator.`)
            }
            const authLoginActions = await this.dbTask.getResourceActionsForAuthLogin()
            if( authLoginActions.length<3 ){
                throw new Error(`Auth controller actions 'login','logout' and default/NULL  must be in table cp__package_instance before running imports.`)
            }
            if( !owners || !tenant || !residents ){
                throw new Error('Owners, tenant & residents user groups are required!')
            }
            const permitResult = await this.dbTask.permitAuthToUserGroups(authLoginActions,[ owners.id, tenant.id, residents.id])
            if( !permitResult ){
                throw new Error(`Failed to allow auth permissions to user groups.`)
            }

            const timeStarted= await this.dbTask.getImportTimeStart()
            if( timeStarted ){
                throw new ImportInstanceAlreadyRunning(`A running import process already started at ${timeStarted}. Please wait until the import completes.`)
            }

            // ! save pre-defined custom fields if not exists
            const dbCustomFields = await this.dbTask.saveCustomFieldsSettings()
            await this.dbTask.saveObjectGroup(dbCustomFields)

        }catch(error:any){
            if( error instanceof ImportInstanceAlreadyRunning) {
                console.log({error})
            }else{
                console.error({error})
            }
            await this.saveAuditTrails({error:error.stack?error.stack.toString():error})
            throw error
        }
    }

    public async sync(inputUnits:object|Array<Object>){
        try{
            if( !this.instanceData.id ){
                throw new Error(`Function 'init()' needs to be called first`)
            }
            let errors:string=''

            const timeStarted= await this.dbTask.getImportTimeStart()
            if( timeStarted ){
                errors=`A running import process already started at ${timeStarted}. Please wait until the import completes.`
                return{
                    success:false,
                    syncComplete: false,
                    errors
                }
            }

            let mappedUsers:Array<Object>=[]
            if( Array.isArray(inputUnits) ){
                mappedUsers = await this.castUnitsToUsersWithCustomFields(inputUnits)
            }else{
                mappedUsers = await this.mapUnitUsersToCustomFields(inputUnits)
            }
            const inputUsers  = mappedUsers.slice() as IJsonUsers   

            await this.dbTask.setImportTimeStart()

            // ! save all units
            await this.saveUnits(inputUsers)

            // !  only allow entries w/ tenant_id
            const jUsers = await(async()=>{
                return _.filter(inputUsers,j=>j.tenant_id) as IJsonUsers
            })()

            // ! get all users from db using emails from JSON
            const allJEmails=await(async()=>{
                return _.filter(jUsers,j=>j.email)
            })()
            const  allEmails=await(async()=>{
                return _.map(allJEmails as IJsonUsers ,m=>m.email as string)
            })()
            const allUniqEmails:string[]= [...new Set(allEmails)] 
            let dbAllEmails: IDbUsers = await this.dbTask.getUsersByEmails(allUniqEmails)

            const userTree = new BinTree((a:number, b:number)=> a - b)
            // ! insert id_account to binary tree
            for await(const u of dbAllEmails){
                if( !u.id_account ) continue
                if ( u.id_account !== u.id_account) continue // ! skip non-numeric
                userTree.insert(parseInt(u.id_account.toString()))
            }

            const dbProfiles: IDbProfiles = await this.dbTask.getProfiles()
            const profileTree = new BinTree((a:number, b:number)=> a - b)
            // ! insert id_account to binary tree
            for await(const p of dbProfiles){
                if( !p.id_account) continue
                if ( p.id_account !== p.id_account) continue // ! skip  non numeric
                profileTree.insert(parseInt(p.id_account.toString()))
            }

            const newJUsers:IJsonUsers=[],oldJUsers:IJsonUsers=[]
            // ! iterate JSON users & match to binary trees
            for await(const j of jUsers){
                if( !j.tenant_id ){
                    continue
                }
                if (j.tenant_id !== j.tenant_id){ // ! non-numeric value
                    continue
                }
                let accountID=parseInt(j.tenant_id)

               if( profileTree.find(accountID) ){ // ! find in profiles
                    oldJUsers.push(j)
                    profileTree.remove(accountID)
                }else{
                    newJUsers.push(j)
                }
            } // * for-of

            let success:boolean=true

            if( newJUsers.length ){
                const createResult = await this.create(newJUsers,allUniqEmails)
                if( _.has(createResult,'errors') ){
                    if( createResult.errors ){
                        success=false
                        errors+=`${createResult.errors}. `
                    }
                }else{
                    success=true
                }
            }
            if( oldJUsers.length ){
                const updateResult = await this.update(oldJUsers)
                if( _.has(updateResult,'errors')) {
                    if( updateResult.errors ){
                        success=false
                        errors+=`${updateResult.errors}. `
                    }else{
                        success=true
                    }
                }
            }

            await this.saveCustomFieldsMetaDataByMapping(mappedUsers)
            await this.saveUsersToRoleGroups(jUsers)
            await this.deactivateUsers(jUsers)
            await this.dbTask.clearCache()
            if( errors ){
                return{
                    success,
                    syncComplete: success,
                    errors
                }
            }else{
                return{
                    success,
                    syncComplete: success
                }
            }
        }catch(error:any){
            console.error(error)
            await this.saveAuditTrails({error:error.stack?error.stack.toString():error})
            throw error
        }
    }

    private async saveUsersToRoleGroups(jUsers:IJsonUsers){
        // ! Save each user to groups (Owners/Tenants & Residents)
        
        // ! Get fresh copy of profiles
        const dbProfiles = await this.dbTask.getProfiles()
        const idAccounts = await(async()=>{
            return _.map(dbProfiles as IDbProfiles,m=>m.id_account as string)
        })()
        const dbUsersGroups =await  this.dbTask.getUsersRoleGroups(this.residentGroupNames)

        // ! Add users to groups based on resident_type
        const residentGroup = await(async()=>{
            return this.residentGroups.find(g=>g.name==ResidentGroup.Residents)
        })()

        const roleGroupsToAdd: IRoleGroups=[]
        for await(const j of jUsers){
            let foundIndex = await(async()=>{
                return _.findIndex(idAccounts,f=>f==j.tenant_id)
            })()
            if( foundIndex===-1 ){
                continue // not found
            }
            idAccounts.splice(foundIndex,1) // ! found,  remove!
            // check if already added to cp__role_group
            // ! Get the id_group by resident type
            let matchedGroup: IResidentGroup|undefined = this.getUserGroupByResidentType(j.resident_type)
            if( !matchedGroup ) {
                continue
            }
            let profile = await (async ()=>{
                return dbProfiles.find( d=>d.id_account==j.tenant_id)
            })()
            if( !profile ){
                continue
            }
            let alreadyAdded: IUsersRoleGroup|undefined
            if(matchedGroup && matchedGroup.name.toUpperCase()!=ResidentGroup.Residents.toUpperCase() ){
                alreadyAdded = await (async()=>{
                    return dbUsersGroups.find(u=>(u. 
                    id_instance==this.id_instance && matchedGroup &&
                    u.id_group==matchedGroup.id && profile &&
                    u.id_role==profile.id_user))
                    })()
                if( !alreadyAdded ){
                    roleGroupsToAdd.push({
                        id_instance: this.id_instance,
                        id_group: matchedGroup.id,
                        id_role: profile.id_user
                    } as IRoleGroup)                
                }                    
            }
            // ! Must: Add each user to 'Residents' group
            alreadyAdded = await (async()=>{
                return dbUsersGroups.find(u=>u. 
                id_instance==this.id_instance && residentGroup &&
                u.id_group==residentGroup.id && profile &&
                u.id_role==profile.id_user)
                })()

            if( !alreadyAdded && residentGroup ){
                roleGroupsToAdd.push({
                    id_instance: this.id_instance,
                    id_group: residentGroup.id,
                    id_role: profile.id_user
                } as IRoleGroup)                
            }                    
        }
        if( roleGroupsToAdd.length ){
            const roleGroupsModel = await this.dbTask.getModel(ModelType.Server,"RoleGroup",RoleGroupAttributes,ServerTable.UserRoleGroup)
            const roleGroupsToAddReversed = roleGroupsToAdd.slice().reverse()
            await this.dbTask.saveBulk(roleGroupsModel,roleGroupsToAddReversed)
        }
    }

    private async saveCustomFieldsMetaData(inputUsers:IJsonUsers){
        // ! get saved custom fields
        const dbCustomFields = await this.dbTask.getCustomFields()
        // ! get saved/stored meta data
        const dbMetaData = await this.dbTask.getCustomFieldsMetaData()
        // if( !dbMetaData.length ) return

        const jValidUnits =await(async()=>{
            return _.filter(inputUsers,j=>j.unit_title) as IJsonUsers
        })()
        const unitTitles= await(async()=>{
            return _.map(jValidUnits,m=> m.unit_title as string)
        })()
        
        // ! create meta data for units
        const dbUnits = await this.dbTask.getUnits()
        const dbUnitsFiltered = await(async()=>{
            return _.filter(dbUnits,u=>unitTitles.includes(u.title as string)?true:false)
        })()
        const metaDataToAdd:ICustomFieldMetaDatas=[]
        const metaDataToUpdate:ICustomFieldMetaDatas=[]
        // ! iterate units
        for await(const unit of dbUnitsFiltered){
            // ! find tenant_id of unit as the leaseID
            const jUsers = await(async()=>{
                return _.filter(inputUsers,j=>j.unit_title==unit.title && j.tenant_id) as IJsonUsers
            })()
            if( !jUsers.length ){
                continue
            }
            // ! iterate custom fields for units
            const dbUnitCustomFields=await(async()=>{
                return dbCustomFields.filter(d=>d.id_entity==EntityTypes.Units.id)
            })()
            if( dbUnitCustomFields.length ){
                for await(const jUser of jUsers){
                    for await(const cField of dbUnitCustomFields ){
                        let foundMetaData:ICustomFieldMetaData|undefined
                        // LEASE ID
                            foundMetaData = await(async()=>{
                                return dbMetaData.find(m=>m.id_entity==EntityTypes.Units.id && m.id_object==unit.id && m.id_field==cField.id )
                            })()
                            if(foundMetaData){
                                // ! Compare values & update if found changes
                                if( foundMetaData.value!=(jUser.unit_id?jUser.unit_id:'')){
                                    metaDataToUpdate.push({
                                        id:foundMetaData.id,
                                        id_entity:EntityTypes.Units.id,
                                        id_field:cField.id,
                                        id_object: unit.id,
                                        value: jUser.unit_id?jUser.unit_id:''
                                    } as ICustomFieldMetaData)
                                }
                            }else{
                                metaDataToUpdate.push({
                                    id_entity:EntityTypes.Units.id,
                                    id_field:cField.id,
                                    id_object: unit.id,
                                    value: jUser.unit_id?jUser.unit_id:''
                                } as ICustomFieldMetaData)
                            }
                    }
                }
            }
        }

        // ! save meta data for units & user/profiles
        if( metaDataToAdd.length || metaDataToUpdate.length ){
            const metaDataModel = await this.dbTask.getModel(ModelType.Customer,"MetaData",CustomFieldsMetaDataAttributes,CustomerTable.MetaData)
            if( metaDataToAdd.length ){
                const metaDataToAddReversed=metaDataToAdd.slice().reverse()
                await this.dbTask.saveBulk(metaDataModel,metaDataToAddReversed)
            }
            if( metaDataToUpdate.length){
                const metaDataToUpdateCopy = metaDataToUpdate.slice()
                await this.dbTask.updateBulk(metaDataModel,metaDataToUpdateCopy,['value'])
            }
        }

    }

    private async saveCustomFieldsMetaDataByMapping(mappedUsers:any){

        // ! get defined custom fields
        const definedCustomFields = await this.dbTask.getDefinedCustomFields()
        // ! get saved custom fields
        const dbCustomFields = await this.dbTask.getCustomFields()
        // ! get saved/stored meta data
        const dbMetaData = await this.dbTask.getCustomFieldsMetaData()

        const mValidUnits =await(async()=>{
            return _.filter(mappedUsers,j=>j.unit_title)
        })()
        const mUnitTitles= await(async()=>{
            return _.map(mValidUnits,m=> m.unit_title as string)
        })()

        const dbUnits = await this.dbTask.getUnits()
        const dbUnitsFiltered = await(async()=>{
            return _.filter(dbUnits,u=>mUnitTitles.includes(u.title as string)?true:false)
        })()

        const metaDataToAdd:ICustomFieldMetaDatas=[]
        const metaDataToUpdate:ICustomFieldMetaDatas=[]

        // ! -------------------------------
        // ! create meta data for units
        // ! -------------------------------

        const definedUnitCustomFields = await(async()=>{
            return _.filter(definedCustomFields,m=>m.id_entity==EntityTypes.Units.id)
        })()

        const dbUnitCustomFields=await(async()=>{
            return dbCustomFields.filter(d=>d.id_entity==EntityTypes.Units.id)
        })()

        if( dbUnitCustomFields.length && definedUnitCustomFields.length ){
            // ! iterate each mapped user
            for await(const mUser of mappedUsers){
                let unit = await(async()=>{
                    return dbUnitsFiltered.find(d=>d.title==mUser.unit_title)
                })()
                if( !unit ){
                    continue
                }
                // ! iterate defined custom fields
                for await(const definedField of definedUnitCustomFields ){
                    let foundDbCustomField = await(async()=>{
                        return dbUnitCustomFields.find(cf=>cf.id_entity==definedField.id_entity && cf.id_fieldtype==definedField.id_fieldtype && cf.tag_label==definedField.tag_label)
                    })()
                    if( !foundDbCustomField){
                        continue
                    }
                    let foundMetaData:ICustomFieldMetaData|undefined = await(async()=>{
                            return dbMetaData.find(d=>d.id_entity==EntityTypes.Units.id && unit && d.id_object==unit.id && foundDbCustomField && d.id_field==foundDbCustomField.id )
                    })()
                    if(foundMetaData){
                        // ! compare values & update if value changed
                        if( foundMetaData.value!=(mUser[`${definedField.map_inputfield}`]??'') ){
                            metaDataToUpdate.push({
                                id:foundMetaData.id,
                                id_entity:EntityTypes.Units.id,
                                id_field:foundDbCustomField.id,
                                id_object: unit.id,
                                value: mUser[`${definedField.map_inputfield}`]??''
                            } as ICustomFieldMetaData)
                        }
                    }else{
                        // ! create meta data
                        metaDataToAdd.push({
                            id_entity:EntityTypes.Units.id,
                            id_field:foundDbCustomField.id,
                            id_object: unit.id,
                            value: mUser[`${definedField.map_inputfield}`]??''
                        } as ICustomFieldMetaData)
                    }
                }
            }
        }

        // !--------------------------------
        // ! create meta data for users
        // !--------------------------------

        const definedUserCustomFields = await(async()=>{
            return _.filter(definedCustomFields,m=>m.id_entity==EntityTypes.Users.id)
        })()

        // ! get custom fields for users
        const dbUserCustomFields=await(async()=>{
            return dbCustomFields.filter(d=>d.id_entity==EntityTypes.Users.id)
        })()

        const mValidUsers =await(async()=>{
            return _.filter(mappedUsers,m=>m.tenant_id)
        })()

        const mAccountIDs= await(async()=>{
            return _.map(mValidUsers,m=> m.tenant_id)
        })()

        const dbProfilesFiltered = await this.dbTask.getProfilesByAccountIDs(mAccountIDs)

        if( dbUserCustomFields.length && definedUserCustomFields.length ){
            // ! iterate each mapped user
            for await(const mUser of mappedUsers){
                let profile = await(async()=>{
                    return dbProfilesFiltered.find(d=>d.id_account==mUser.tenant_id)
                })()
                if( !profile ){
                    continue
                }
                // ! iterate defined custom fields
                for await(const definedField of definedUserCustomFields ){
                    let foundDbCustomField = await(async()=>{
                        return dbUserCustomFields.find(cf=>cf.id_entity==definedField.id_entity && cf.id_fieldtype==definedField.id_fieldtype && cf.tag_label==definedField.tag_label)
                    })()
                    if( !foundDbCustomField){
                        continue
                    }
                    let foundMetaData:ICustomFieldMetaData|undefined = await(async()=>{
                            return dbMetaData.find(d=>d.id_entity==EntityTypes.Users.id && profile && d.id_object==profile.id_user && foundDbCustomField && d.id_field==foundDbCustomField.id )
                    })()
                    if(foundMetaData){
                        // ! compare values & update if value changed
                        if( foundMetaData.value!=(mUser[`${definedField.map_inputfield}`]??'') ){
                            metaDataToUpdate.push({
                                id:foundMetaData.id,
                                id_entity:EntityTypes.Users.id,
                                id_field:foundDbCustomField.id,
                                id_object: profile.id_user,
                                value: mUser[`${definedField.map_inputfield}`]??''
                            } as ICustomFieldMetaData)
                        }
                    }else{
                        // ! create meta data
                        metaDataToAdd.push({
                            id_entity:EntityTypes.Users.id,
                            id_field:foundDbCustomField.id,
                            id_object: profile.id_user,
                            value: mUser[`${definedField.map_inputfield}`]??''
                        } as ICustomFieldMetaData)
                    }
                }
            }
        }

        // ! save meta data for user
        if( metaDataToAdd.length || metaDataToUpdate.length ){
            const metaDataModel = await this.dbTask.getModel(ModelType.Customer,"MetaData",CustomFieldsMetaDataAttributes,CustomerTable.MetaData)
            if( metaDataToAdd.length ){
                const metaDataToAddReversed=metaDataToAdd.slice().reverse()
                await this.dbTask.saveBulk(metaDataModel,metaDataToAddReversed)
            }
            if( metaDataToUpdate.length){
                const metaDataToUpdateCopy = metaDataToUpdate.slice()
                await this.dbTask.updateBulk(metaDataModel,metaDataToUpdateCopy,['value'])
            }
        }

    }

    private async deactivateUsers(inputUsers:IJsonUsers){
        // ! only allow entries w/ tenant_id
        const jUsers = await(async()=>{
            return _.filter(inputUsers,j=>j.unit_id && j.unit_title) as IJsonUsers
        })()
        if( !jUsers.length ){
           return {
               errors: `All entries are  required to have valid tenant_id`
           }
       }
       const accountIDs = await(async()=>{
           return jUsers.map(m=>m.tenant_id as string)
       })()
       const uniqAccountIDs = [...new Set(accountIDs)]
       // ! get db users by accountIDs
       const dbUserProfiles=await this.dbTask.getUserProfilesByAccountIDs(uniqAccountIDs)
       // ! get all units
       const dbUnits = await this.dbTask.getUnits()
       interface IJUnitUser {unit_title:string,id_unit:number,id_user:number,account_id:string}
       // ! associate the inputs with units+users
       const jDbUnitUsers:Array<IJUnitUser>=[]
       for await(const j of jUsers){
           const unit = await(async()=>{
               return dbUnits.find(u=>u.title==j.unit_title)
           })()
           const user = await(async()=>{
               return dbUserProfiles.find(u=>u.id_account==j.tenant_id)
           })()
           if( !unit || !user ){
               continue
           }
           jDbUnitUsers.push({
               unit_title: j.unit_title,
               id_unit: unit.id,
               id_user: user.id,
               account_id: j.tenant_id
           } as IJUnitUser )
       }
       if( !jDbUnitUsers.length){
           return{
               errors: 'Did not match any existing record.'
           }
       }
       const aList =  await(async()=>{
        return jDbUnitUsers.filter(f=>f.unit_title=='1111')
        })()
       // ! get all unit users
       const dbUnitProfiles = await this.dbTask.getUnitProfiles()
       const unitProfilesToRemove:Array<number>=[]
       // ! iterate the full list
       for await(const db of dbUnitProfiles){
           let found = await(async()=>{
               return jDbUnitUsers.find(f=>f.id_unit==db.id_unit && f.id_user==db.id_user)
           })()
           if( !found ){
               unitProfilesToRemove.push(db.id as number)
           }
       }
       if( unitProfilesToRemove.length ){
           await this.dbTask.hardDeleteUnitProfilesByIDs(unitProfilesToRemove)
       }

       const allJEmails=await(async()=>{
           return _.filter(jUsers,j=>j.email) as IJsonUsers
       })()
       const  allEmails=await(async()=>{
           return allJEmails.map(m=>m.email as string)
       })()
       const allUniqEmails:string[]= [...new Set(allEmails)] 
       await this.dbTask.softDeleteOldProfilesWithDuplicateEmails(jUsers,allUniqEmails)
       
       // ! extract just the id_user & make them unique
       const mappedDbUsers=await(async()=>{
           return jDbUnitUsers.map(m=>m.id_user)
       })()
       const jIDUsers:Array<number>=[...new Set(mappedDbUsers)]
       const profilesToDeactivate:IDbProfiles=[]
       const profileIDsToSoftDelete:Array<number>=[]

       const dbProfiles = await this.dbTask.getProfilesWithAccountIDs()

       const mappedDbProfiles=await(async()=>{
           return dbProfiles.map(m=>m.id_user as number)
       })()
       const profileUserIDs:Array<number>=[...new Set(mappedDbProfiles)]

       const dbUserGroups:IUserGroups = await this.dbTask.getUsersGroupsOfUserIDs(profileUserIDs)
       const dbAdminGroups = await(async()=>{
           return dbUserGroups.filter(d=>d.groupname && ![ResidentGroup.Owners.toString(),ResidentGroup.Residents.toString(),ResidentGroup.Tenant.toString()].includes(d.groupname))
       })()

       for await(const p of dbProfiles ){
           // ! skip profile if found in admin groups
           let admin = await(async()=>{
               return dbAdminGroups.find(d=>d.id_role==p.id_user)
           })()
           if( admin ){
               continue
           }
           // ! check if DB user is still in the input list
           let found = await(async()=>{
               return jIDUsers.find(id_user=>p.id_user==id_user)
           })()
           if( !found ){
               profilesToDeactivate.push(p)
               profileIDsToSoftDelete.push(p.id as number)
           } 
       }
       // ! deactivate profiles
       if(profileIDsToSoftDelete.length){
           await this.dbTask.softDeleteProfilesByIDs(profileIDsToSoftDelete)
       }

       // !deactivate in mds_transactions
       if( profilesToDeactivate.length){
           await this.dbTask.deactivateMdsTransactionsByProfile(profilesToDeactivate)
       }
   }

    private async create(inputUsers:IJsonUsers,allUniqEmails:string[]){
        try{
            if( !this.instanceData ) throw new Error('invalid instanceData')

            // ! only allow entries w/ tenant_id
            const jUsers = await(async()=>{
                return _.filter(inputUsers,j=>j.tenant_id) as IJsonUsers
            })()
            if( !jUsers.length ){
                return {
                    success:false,
                    errors: `There are no residents/tenants that have valid tenant_id.`
                }
            }

            this.setFormattedNames(jUsers)


            let dbProfiles: IDbProfiles = await this.dbTask.getProfiles()
            let dbAllEmails: IDbUsers = await this.dbTask.getUsersByEmails(allUniqEmails)
            let dbUnits = await this.dbTask.getUnits()

            const now = dayjs()
            const nowTimeStamp = now.format('X')

            // ! Filter with emails only
            const jUsersWithEmail: IJsonUsers = await(async()=>{
                return _.filter(jUsers,v=>v.email) as IJsonUsers
            })()

            const newEmailsToAdd:IDbUsers=[]
            const newEmailsSet = new Set()
            // ! Iterate to match users w/ email
            for await(const j of jUsersWithEmail){
                // ! only query valid emails
                if( !j.email ) continue

                // ! skip if already added
                if( newEmailsSet.has(j.email.toLowerCase()) ) {
                    continue
                }

                // ! match in email list
                let user = await(async()=>{
                    return dbAllEmails.find(d=>
                        d.username && j.email &&
                        d.username.trim().toLowerCase()==j.email
                    )
                })()
                // ! if not found, query email in db profiles
                if( !user ){
                    let profile = await(async()=>{
                        return dbProfiles.find(d=>
                            d.email && j.email &&
                            d.email.trim().toLowerCase()==j.email
                        )
                    })()
                    if( profile ){

                        // let displayName:
                        newEmailsToAdd.push({
                            id: profile.id_user,
                            username: j.email.trim().toLowerCase(),
                            email: j.email.trim().toLowerCase(),
                            display_name: j.formattedName.display_name,
                            is_valid_email: 1
                            // id_account: null // ! IMPORTANT: architecture of users can only have one email/username even with multitple id_account(s)
                        })
                        newEmailsSet.add(j.email.trim().toLowerCase())
                        continue
                    }
                }
                // ! otherwise add to save new user
                if(!user){
                    newEmailsToAdd.push({
                        username: j.email.trim().toLowerCase(),
                        email: j.email.trim().toLowerCase(),
                        display_name: j.formattedName.display_name,
                        is_valid_email: 1
                        // id_account: null // ! IMPORTANT: architecture of users can only have one email/username even with multitple id_account(s)
                    })
                    newEmailsSet.add(j.email.trim().toLowerCase())
                }
            } // * for-of 

            const modelUsers=await this.dbTask.getModel(ModelType.Server,'User',UserAttributes,ServerTable.Users)

            // ! Save new users
            if( newEmailsToAdd.length ){
                // clone & reverse
                const newEmailsToAddReversed = newEmailsToAdd.slice().reverse()

                const newEmails = await(async()=>{
                    return _.map(newEmailsToAdd,m=> m.email as string )
                })()
                await this.dbTask.saveBulk(modelUsers,newEmailsToAddReversed)
                if( newEmails.length ){
                    const savedUsers= await this.dbTask.getUsersByEmails(newEmails)
                }

                // ! get fresh copy/repopulate
                dbAllEmails = await this.dbTask.getUsersByEmails(allUniqEmails)
            }

            interface INewAccount{
                email:string,
                account_id:string,
                display_name:string
            }

            interface INewAccounts extends Array<INewAccount>{}

            const newProfilesWithEmail:IDbProfiles=[]
            const newAccounts:INewAccounts=[]
            // ! Iterate to match profiles w/ email
            for await(const j of jUsersWithEmail){

                // ! find user using email
                let user = await(async()=>{
                    return dbAllEmails.find(d=>d.email?.trim().toLowerCase()==j.email && j.email) // CORE-7377
                })()

                if( !user ) {
                    // ! all emails should have been saved earlier
                    continue
                }
                
                // ! skip if already added to list
                let accountFound = await(async()=>{
                    return newAccounts.find(a=>a.account_id==j.tenant_id && a.email.trim().toLowerCase()==j.email && j.email  && a.display_name && a.display_name.toString().toUpperCase()==(j.formattedName.display_name).toUpperCase())
                })()
                if ( accountFound ){
                    continue
                }

                // ! check if already exists
                let profile = await(async()=>{
                    return dbProfiles.find(d=>d.id_account==j.tenant_id)
                })()

                // ! add to new profiles if not found
                if(!profile ){
                    let p={
                        id_community: this.instanceData.id_community,
                        id_user: user.id,
                        first_name: j.formattedName.first_name,
                        last_name: j.formattedName.last_name,
                        display_name: j.formattedName.display_name,
                        email: j.email?j.email:null,
                        registered_date: nowTimeStamp,
                        id_language: 5,
                        is_acl_actived: 1,
                        id_account: j.tenant_id,
                        is_valid_email: j.email?1:0,
                        is_local:j.email?0:1,
                        is_deleted: 0,
                        phone: null,
                        is_valid_home_phone:0,
                        cell_phone: null,
                        is_valid_cell_phone:0
                    } as TDbProfile 

                    if( j.phone ){
                        let jPhone=parsePhone(j.phone,true)
                        if(typeof jPhone === 'string'){
                            p.phone=jPhone
                            p.is_valid_home_phone=1
                        }
                    }
                    if( j.cell_phone ){
                        let jCellPhone=parsePhone(j.cell_phone,true)
                        if(typeof jCellPhone === 'string'){
                            p.cell_phone=jCellPhone
                            p.is_valid_cell_phone=1
                        }
                    }
                    newProfilesWithEmail.push(p)
                    newAccounts.push({
                        account_id:  p.id_account,
                        email: p.email?p.email:'',
                        display_name:p.display_name
                    } as INewAccount)
                }
            } // * for-of

            // ! Save new profiles w/ email
            const profileModel = await this.dbTask.getModel(ModelType.Customer,'Profile',ProfilerAttributes,CustomerTable.Profiles)

            if( newProfilesWithEmail.length ){
                const newProfilesWithEmailReversed = newProfilesWithEmail.slice().reverse()
                await this.dbTask.saveBulk(profileModel,newProfilesWithEmailReversed)
            }

            // ! Filter w/out emails
            const jUsersNoEmail = await(async()=>{
                return _.filter(jUsers,j=>j.tenant_id && !j.email ) as 
                IJsonUsers
            })()
            const allAccountIDs = await(async()=>{
                return _.map(jUsers,m=> m.tenant_id as string)
            })()
            const allUniqAccountIDs = [...new Set(allAccountIDs)]
            const dbProfilesWithAccountID = await this.dbTask.getProfilesByAccountIDs(allUniqAccountIDs)

            const newUsersNoEmail:IDbUsers=[],newUsersAccountID:string[]=[]
            // ! Iterate to match users/profiles
            for await( const j of jUsersNoEmail){

                if( !j.tenant_id ) continue

                let userAccountIdFound= await(async()=>{
                    return newUsersAccountID.find(a=>a==j.tenant_id)
                })()
                if( userAccountIdFound  ) continue // * already added earlier

                // ! match in profiles that has no id_user
                let profile = await(async()=>{
                    return dbProfilesWithAccountID.find(d=>
                        d.id_account && d.id_account==j.tenant_id && !d.id_user )
                })()
                if( profile ){
                    // ! found in profiles but not in users/roles (perhaps removed), re-insert
                    newUsersNoEmail.push({
                        id: profile.id_user,
                        display_name: j.formattedName.display_name,
                        is_valid_email: 0,
                        id_account: j.tenant_id // ! temporary reference
                    })
                    newUsersAccountID.push(j.tenant_id)
                    continue
                }
                // ! otherwise add to save new user
                newUsersNoEmail.push({
                    display_name:  j.formattedName.display_name,
                    is_valid_email: 0,
                    id_account: j.tenant_id // ! temporary reference
                })
                newUsersAccountID.push(j.tenant_id)
            } // * for-of

            let dbUsersWithAccountID:IDbUsers=[]

            // ! Save new users w/out emails
            if( newUsersNoEmail.length ){

                const newUsersNoEmailReversed = newUsersNoEmail.slice().reverse()
                const newAccountIDs = await(async()=>{
                    return _.map(newUsersNoEmail,m=>m.id_account as 
                        string )
                })()
                await this.dbTask.saveBulk(modelUsers,newUsersNoEmailReversed)

                // ! Get fresh copy to include id_user of newly saved users
                dbUsersWithAccountID = await this.dbTask.getUsersByAccountIDs(allUniqAccountIDs)
            }

            const newProfilesNoEmail:IDbProfiles=[]
            // ! Iterate to match profiles w/out email                
            for await(const j of jUsersNoEmail){
                if( !j.tenant_id ) continue
                if( j.tenant_id.toString().trim().length == 0 ) continue

                // ! skip if already added to list
                let accountFound = await(async()=>{
                    return newAccounts.find(a=>a.account_id==j.tenant_id && a.email.trim().toLowerCase()==j.email && j.email  && a.display_name && a.display_name.toString().toUpperCase()==`${j.first_name??''} ${j.last_name??''}`.toUpperCase())
                })()
                if ( accountFound  ){
                    continue
                }

                let profile = await(async()=>{
                    return dbProfilesWithAccountID.find(d=>
                        d.id_account && d.id_account==j.tenant_id 
                    )
                })()
                if( !profile ){
                    // ! Not in profiles, search user
                    let user = dbUsersWithAccountID.find(d=>d.id_account==j.tenant_id)
                    const userID=user?user.id:null
                    let p={
                        first_name: j.formattedName.first_name,
                        last_name: j.formattedName.last_name,
                        display_name: j.formattedName.display_name,
                        email: j.email?j.email:null,
                        registered_date: nowTimeStamp,
                        id_language: 5,
                        is_acl_actived: 1, // CORE-7419  /* j.email?1:0, // CORE-7528 */
                        id_account: j.tenant_id,
                        is_valid_email: j.email?1:0,
                        is_local:j.email?0:1,
                        is_deleted: 0
                    } as TDbProfile

                    if( userID ){
                        p.id_user=userID
                    }
                    if( j.phone ){
                        let jPhone=parsePhone(j.phone,true)
                        if(typeof jPhone === 'string'){
                            p.phone=jPhone
                            p.is_valid_home_phone=1
                        }
                    }
                    if( j.cell_phone ){
                        let jCellPhone=parsePhone(j.cell_phone,true)
                        if(typeof jCellPhone === 'string'){
                            p.cell_phone=jCellPhone
                            p.is_valid_cell_phone=1
                        }
                    }
                    newProfilesNoEmail.push(p)
                    newAccounts.push({
                        account_id:  j.tenant_id,
                        email: j.email?j.email:'',
                        display_name: j.formattedName.display_name
                    } as INewAccount)
                }
            } // * for-of

            // ! Save profiles w/out email
            if( newProfilesNoEmail.length ){
                const newProfilesNoEmailReversed = newProfilesNoEmail.slice().reverse()
                const newProfileAccountIDs = await(async()=>{
                    return _.map(newProfilesNoEmail,m=> m.id_account as string )
                })()
                await this.dbTask.saveBulk(profileModel,newProfilesNoEmailReversed)
            }

            // ! get a fresh copy from DB
            dbUnits = await this.dbTask.getUnits()

            // ! get unit profiles from DB from the unit titles of JSON
            const filteredUnits = await(async()=>{
                return _.filter(jUsers,f=>f.unit_title)
            })()
            const  unitsInJSON = await(async()=>{
                return _.map(filteredUnits as IJsonUsers, m=>m.unit_title as string)
            })()
            const uniqUnitsInJSON =  [...new Set(unitsInJSON)]
            const dbUnitProfiles = await this.dbTask.getUnitProfilesByUnitTitles(uniqUnitsInJSON)
            
            // ! Get fresh copy/repopulate
            dbAllEmails = await this.dbTask.getUsersByEmails(allUniqEmails)

            // ! Get fresh copy/repopulate
            const dbUserProfiles = await this.dbTask.getUserProfilesByAccountIDs(allUniqAccountIDs)

            type TjUser={
                id_user: number|string,
                unit_title: string,
                unit_id: number|string, // as leaseID
                tenant_id?: number|string                    
            }
            interface IjUsers extends Array<TjUser>{}

            const jMatchedUsers:IjUsers=[]
            const newUnitProfiles:IDbUnitProfiles=[]
            // ! Iterate all json users
            for await(const j of jUsers){
                if( !j.unit_title ){
                    continue
                }
                let user:TDbUser|undefined
                // ! check if tenant_id is valid
                if( !j.tenant_id ){
                    // ! match email instead
                    if( !j.email ) {
                        continue
                    }
                    // ! match the email
                    user =await(async()=>{
                        return dbAllEmails.find(d=>d.email && d.email?.trim().toLowerCase()==j.email && j.email)
                    })()
                }else{
                    // ! match account ID
                    user = await(async()=>{
                        return dbUserProfiles.find(d=>d.id_account && d.id_account==j.tenant_id)
                    })()
                }
                if( !user ){
                    continue
                }

                // ! match unit title to db unit list
                let unit=await(async()=>{
                    return dbUnits.find(d=>d.title==j.unit_title)
                })()
                if( !unit ){
                    continue
                }

                // ! check if already added
                let matchedUserFound =  await(async()=>{
                    return jMatchedUsers.find(m=>user && m.id_user==user.id && m.unit_title==j.unit_title && m.tenant_id==j.tenant_id)
                })()
                if( matchedUserFound ){
                    continue
                } 

                jMatchedUsers.push({
                    id_user: user.id,
                    unit_title: unit.title,
                    unit_id: j.unit_id,
                    tenant_id: j.tenant_id
                } as TjUser)
 
                // ! match if user was already added to unit profile
                let unitProfile= await(async()=>{
                    return dbUnitProfiles.find(d=>d.id_unit  && d.id_user  && d.title && user && d.id_user==user.id && j.unit_title && d.title==j.unit_title)
                })()
                if( !unitProfile ){
                    if( !unit && !user ) {
                        continue
                    }
                    newUnitProfiles.push({
                        id_community: this.instanceData.id_community,
                        id_unit: unit.id,
                        id_user: Number(user.id),
                        is_resident:  j.is_resident??1,
                        id_group_type: DbTask.getGroupTypeID(j.resident_type,j.is_resident??1),
                        id_resident_type: DbTask.getResidentTypeID(j.resident_type),
                        created_date: nowTimeStamp
                    })
                }

            } // * for-of

            // ! save unit profiles
            if( newUnitProfiles.length ){
                const newUnitProfilesReversed=newUnitProfiles.slice().reverse()
                const modelUnitProfiles = await this.dbTask.getModel(ModelType.Customer,'UnitProfile',UnitProfileAttributes,CustomerTable.UnitProfiles)
                await this.dbTask.saveBulk(modelUnitProfiles,newUnitProfilesReversed)
            }

            const createdDate=now.format('YYYY-MM-DD')

            const mdsTransInsert:IMdsTransactions = []
            const mdsTrans = await this.dbTask.getMdsTransactions()
            for await(const jm of jMatchedUsers){
                let foundMdsTran = await(async()=>{
                    return mdsTrans.find(t=>t.id_community && t.id_community==this.instanceData.id_community && t.id_user && t.id_user==jm.id_user && jm.tenant_id && jm.tenant_id==t.mds_contract_record)
                })()
                if( !foundMdsTran ){
                    mdsTransInsert.push({
                        id_community: this.instanceData.id_community,
                        id_user: jm.id_user,
                        leaseid: jm.unit_id,
                        mds_contract_record: jm.tenant_id,
                        is_active: 1,
                        created_date: createdDate ,
                        stop: 0
                    } as TMdsTransaction)
                }
            } // * for-of

            if( mdsTransInsert.length ){
                const mdsTransInsertReversed=mdsTransInsert.slice().reverse()
                const mdsTransModel = await this.dbTask.getModel(ModelType.Customer,'MdsTrans',MdsTransactionAttributes,CustomerTable.MdsTransactions)
                await this.dbTask.saveBulk(mdsTransModel,mdsTransInsertReversed)
            }

            // clear temporary reference
            await this.dbTask.clearAccountIDsFromUsers(newUsersAccountID)

            return{
                success:true,
                importComplete:true
            }

        }catch(error){
            throw error
        }
    }

    private async saveUnits(jUsers:IJsonUsers){
        try{
            if( !this.instanceData ) throw new Error('invalid instanceData')
            const dbUnits = await this.dbTask.getUnits()
            const dbUnitTitles =await(async()=>{
                return _.map(dbUnits,m=>m.title as string)
            })()
            const jValidUnits = await(async()=>{
                return _.filter(jUsers,f=>f.unit_title && f.unit_title.trim().length)
            })()
            if( !jValidUnits.length ){
                return
            }
            const unitsInJSON=await(async()=>{
                return _.map(jValidUnits as IJsonUsers,m=>m.unit_title as string)
            })()
            if( !unitsInJSON.length ){
                return
            }
            const uniqUnitsInJSON = [...new Set(unitsInJSON)]
            const commonUnits = _.intersection(uniqUnitsInJSON,dbUnitTitles)
            const unitsCreateList = _.difference(uniqUnitsInJSON,commonUnits)
            const existingUnits = _.difference(commonUnits,uniqUnitsInJSON)
            Array.prototype.push.apply(existingUnits,commonUnits); 
            const unitsToUpdate:IDbUnits=[]
            for await(const unitTitle of existingUnits){
                let foundUnit = await(async()=>{
                    return dbUnits.find(d=>d.title==unitTitle)
                })()
                if( foundUnit ){
                    if( foundUnit.is_active==0 ){
                        foundUnit.is_active=1
                        unitsToUpdate.push(foundUnit)
                    }
                }
            }
            const unitsToAdd = await(async()=>{
                return _.map(unitsCreateList,m=>{
                    return {
                        id_community: this.instanceData.id_community,
                        title: m,
                        is_active: 1
                    } as TDbUnit
                })
            })()
            const unitsToAddReversed=unitsToAdd.slice().reverse()
            const modelUnits = await this.dbTask.getModel(ModelType.Customer,'Unit',UnitAttributes,CustomerTable.Units)
            await  this.dbTask.saveBulk(modelUnits,unitsToAddReversed)
            await this.dbTask.updateBulk(modelUnits,unitsToUpdate,['is_active'])
        }catch(error){
            console.error(error)
            throw error
        }
    }

    private async update(inputUsers:IJsonUsers){
        try{
            if( !this.instanceData ) throw new Error('invalid instanceData')

            // ! only allow entries w/ tenant_id
            const jUsers = await(async()=>{
                return _.filter(inputUsers,j=>j.tenant_id) as IJsonUsers
            })()
            if( !jUsers.length ){
                return {
                    success:false,
                    errors: `There are no residents/tenants that have valid tenant_id.`
                }
            }

            this.setFormattedNames(jUsers)

            const now = dayjs()
            const nowTimeStamp = now.format('X')
            const currentDate=now.format('YYYY-MM-DD')

            const jAccountIDs = await(async()=>{
                return _.map(jUsers,m=>m.tenant_id as string)
            })()
            const allJEmails=await(async()=>{
                return _.filter(jUsers,j=>j.email) as IJsonUsers
            })()
            const allUniqJEmails = await(async()=>{
                return [...new Set(_.map(allJEmails,m=>m.email as string))]
            })()
            
            // ! pull existing users by id_account
            let dbUserProfiles  = await this.dbTask.getUserProfilesByAccountIDsOrEmails(jAccountIDs,allUniqJEmails)
            const uProfiles = await(async()=>{
                return dbUserProfiles.filter(up=>up.id_account==62||up.id_account==63||up.id_account==73)
            })()

            // ! pull existing profiles by id_account
            const dbProfiles = await this.dbTask.getProfilesByAccountIDs(jAccountIDs)

            const unitsInJSON = await(async()=>{
                return _.map(jUsers,m=>m.unit_title as string)
            })()
            const uniqUnitsInJSON = [...new Set(unitsInJSON)]
            const dbUnitProfiles = await this.dbTask.getUnitProfilesByUnitTitles(uniqUnitsInJSON)
            interface IDbUserUnit extends TDbUser{
                unit_title:string,
                resident_type: string,
                id_unit: number,
                is_resident: number| boolean
            }

            const unitProfilesCreateList:Array<IDbUserUnit>=[]
            const unitProfilesUpdateList:IDbUnitProfiles=[]
            const roleGroupsToRemove:IRoleGroups=[]
            const usersToAdd:IDbUsers=[]
            const usersToUpdate:IDbUsers=[]

            const ownerTypeIDs:number[]=[ProfileTypes.ResidentOwner.id,ProfileTypes.NonResidentOwner.id]
            const ownerGroupID:number=await(async()=>{
                const group=this.residentGroups.find(f=>f.name.toUpperCase()==ResidentGroup.Owners.toUpperCase())
                return group?group.id:0
            })()
            const tenantGroupID:number=await(async()=>{
                const group= this.residentGroups.find(f=>f.name.toUpperCase()==ResidentGroup.Tenant.toUpperCase())
                return group?group.id:0
            })()

            let newAccountIDs:string[]=[] 
            const newUsersNoEmail:IDbUsers=[]
            let newUsersWithAccountIDs:IDbUsers=[]
            // ! iterate to create/update users   
            for await(const j of jUsers){
                let userProfile = await (async()=>{
                        return dbUserProfiles.find(d=>d.id_account==j.tenant_id.toString()) // BG-1500
                })()
                if( !userProfile ) {
                    continue
                }

                // ! if email changed, add user
                if( j.email ){
                    if( 
                        (userProfile.user_email?userProfile.user_email:null) !== (j.email?j.email:null) || 
                        (userProfile.profile_email?userProfile.profile_email:null) !== (j.email?j.email:null)
                    ){
                        let newUser={
                            username: j.email?j.email.trim().toLowerCase():null,
                            email: j.email?j.email.trim().toLowerCase():null,
                            display_name: j.formattedName.display_name,
                            is_valid_email: j.email?1:0
                        }
                        usersToAdd.push(newUser as TDbUser)                       
                    }

                }else{
                    // ! input email is not valid but a profile requires a user to link itself before assigning to a unit
                    if( userProfile.profile_email==null && userProfile.user_email!=null ){ // BG-1500
                        let alreadyAdded = await (async()=>{
                            return newUsersNoEmail.find(d=>d.display_name==j.formattedName.display_name &&  d.id_account==j.tenant_id.toString()
                            )
                        })() 
                        if( !alreadyAdded ){
                            // ! create a new user & link the profile to this user..
                            newUsersNoEmail.push({
                                display_name: j.formattedName.display_name,
                                is_valid_email: 0,
                                id_account: j.tenant_id // ! temporary reference
                            })                            
                        }
                    }else( userProfile.user_email || userProfile.profile_email ) // CORE-7419
                        {
                            let matchedUsers = await this.dbTask.getUsersByDisplayName(j.formattedName.display_name)
                            if( !matchedUsers.length ){
                                newUsersNoEmail.push({
                                    display_name: j.formattedName.display_name,
                                    is_valid_email: 0,
                                    id_account: j.tenant_id // ! temporary reference
                                })            
                            }
                    }
                }
                // ! check field changes
                let existingUserProfile={
                    id: userProfile.id, // ! MUST!
                    username: j.email?j.email.trim().toLowerCase():null,
                    email: j.email?j.email.trim().toLowerCase():null,
                    display_name: j.formattedName.display_name,
                    is_valid_email: j.email?1:0
                }
                let rxUserDisplayName = new RegExp(`\s?${userProfile.user_display_name}\s?$`, 'gi')
                let rxProfileDisplayName = new RegExp(`\s?${userProfile.profile_display_name}\s?$`, 'gi')
                if( !( j.formattedName.display_name.match(rxUserDisplayName)) || 
                !( j.formattedName.display_name.match(rxProfileDisplayName)) ){ // ! name has changed in user or profile
                    usersToUpdate.push(existingUserProfile  as TDbUser)
                }
            } // * for-of

            if( usersToAdd.length || usersToUpdate.length || newUsersNoEmail.length ){
                const modelUsers=await this.dbTask.getModel(ModelType.Server,'User',UserAttributes,ServerTable.Users)
                // ! save new users
                if( usersToAdd.length ){
                    const usersToAddReversed=usersToAdd.slice().reverse()
                    await this.dbTask.saveBulk(modelUsers,usersToAddReversed)
                    // ! get fresh copy
                    const newDbUserProfiles = await this.dbTask.getUserProfilesByAccountIDsOrEmails(jAccountIDs,allUniqJEmails)                      
                    const filteredNewUsers:IDbUsers  = []
                    for await(const nUserProfile of newDbUserProfiles){
                        let jUser = await(async()=>{
                            return jUsers.find(j=>j.tenant_id==nUserProfile.id_account || j.email==nUserProfile.user_email?.trim().toLowerCase())
                        })()
                        if( !jUser ){
                            // * skip if not found in JSON list
                            continue
                        }
                        // ! if valid email, match it
                        if( nUserProfile.user_email ){
                            if( nUserProfile.user_email?.trim().toLowerCase()==jUser.email ){
                                filteredNewUsers.push(nUserProfile)
                            }
                        }else{
                            // ! otherwise include
                            filteredNewUsers.push(nUserProfile)
                        }
                    }
                }
                // ! update existing users
                if( usersToUpdate.length ){
                    const usersToUpdateCopy=usersToUpdate.slice()
                    await this.dbTask.updateBulk(modelUsers,usersToUpdateCopy,['display_name'])
                }

                // ! Save new users w/out emails
                if( newUsersNoEmail.length ){
                    const newUsersNoEmailReversed = newUsersNoEmail.slice().reverse()
                    newAccountIDs = await(async()=>{
                        return _.map(newUsersNoEmail,m=>m.id_account as 
                            string )
                    })()
                    await this.dbTask.saveBulk(modelUsers,newUsersNoEmailReversed)
                    // ! Get fresh copy to include id_user of newly saved users
                    newUsersWithAccountIDs = await this.dbTask.getUsersByAccountIDs(newAccountIDs)
                }
            }

            // ! get fresh copy
            dbUserProfiles = await this.dbTask.getUserProfilesByAccountIDsOrEmails(jAccountIDs,allUniqJEmails)
            const dbMdsTrans=await this.dbTask.getMdsTransactionsByAccountIDs(jAccountIDs)

            const mdsTransInsert:IMdsTransactions = [],mdsTransUpdate:IMdsTransactions=[]
            const profilesToUpdate:IDbProfiles=[]

            // ! iterate to update profiles
            for await(const j of jUsers){
                let userProfile = await(async()=>{
                    return dbUserProfiles.find(d=>d.id_account==j.tenant_id) // BG-1500
                })()
                if( !userProfile ){
                    continue
                }
                let profile = await(async()=>{
                    return dbProfiles.find(d=>d.id_account==j.tenant_id) // BG-1500
                })()
                if( !profile ) {
                    continue
                }
                let matchedUser:TDbUser|undefined
                let hasChanged:boolean=false,emailChanged:boolean=false
                // ! if was previously inactive
                if( profile.is_deleted ){
                    hasChanged=true
                }

                // ! check if name changed
                if( 
                    profile.first_name!=(j.formattedName.first_name) || 
                    profile.last_name!=(j.formattedName.last_name) ){
                    hasChanged=true
                }
                // ! check if email changed
                if( j.email ){
                    if( profile.email!=userProfile.user_email|| profile.email!=j.email?j.email:null||userProfile.user_email!=j.email?j.email:null ){
                        hasChanged=true
                        emailChanged=true
                        // ! change userProfile using the new email
                        const userProfileOfEmail = await(async()=>{
                            return dbUserProfiles.find(d=>d.user_email?.trim().toLowerCase()== j.email)
                        })()
                        if( userProfileOfEmail ){
                            userProfile=_.clone(userProfileOfEmail)
                        }
                    }
                }else{
                    if( (userProfile.profile_email==null && userProfile.user_email!=null) /* BG-1500 */ ||
                        (userProfile.profile_email && userProfile.profile_email!=j.email) /* CORE-7403, 7419 */ ){ 
                        hasChanged=true
                        matchedUser = await(async()=>{
                            return newUsersWithAccountIDs.find(d=>d.id_account==j.tenant_id)
                        })()
                        if(!matchedUser){
                            // ! get the old profile by account ID
                            let oldProfile = await(async()=>{
                                return dbProfiles.find(d=>d.id_account==j.tenant_id)
                            })()
                            if( oldProfile && oldProfile.display_name ){
                                // ! get the display name from users
                                let matchedUsers = await this.dbTask.getUsersByDisplayName(oldProfile.display_name)
                                if( matchedUsers.length ){
                                    matchedUser = matchedUsers[0]
                                }
                            }
                        }
                    }
                }

                // ! check if phone changed
                let jPhone=parsePhone(j.phone,true)
                if( profile.phone!=(jPhone?jPhone:null) ) {
                    hasChanged=true
                }
                // ! check if cell phone changed
                let jCellPhone=parsePhone(j.cell_phone,true)
                if( profile.cell_phone!=(jCellPhone?jCellPhone:null) ) {
                    hasChanged=true
                }
                if( hasChanged ){
                    let p={
                        id: profile.id, // must
                        id_user: matchedUser?matchedUser.id:userProfile?.id, // BG-1500
                        first_name: j.formattedName.first_name,
                        last_name: j.formattedName.last_name,
                        display_name: j.formattedName.display_name,
                        email: j.email?j.email:null,
                        is_valid_email: j.email?1:0,
                        is_local:j.email?0:1, // CORE-7403
                        is_acl_actived: 1, // CORE-7419 /* j.email?1:0, // CORE-7528 */
                        phone: jPhone,
                        is_valid_home_phone: jPhone?1:0,
                        cell_phone: jCellPhone,
                        is_valid_cell_phone: jCellPhone?1:0,
                        is_deleted: 0
                    } as TDbProfile
                    profilesToUpdate.push(p)
                }

                //  ! check if account ID exists in MDS transactions
                let foundMdsTrans=await(async()=>{
                    return dbMdsTrans.find(d=>j.tenant_id && d.mds_contract_record==j.tenant_id)
                })()
                if( foundMdsTrans ){
                    // ! check if id_community changed
                    if( foundMdsTrans.id_community!=this.instanceData.id_community  ){
                        hasChanged=true
                    }
                    // !if email changed, update mds record
                    if( emailChanged || hasChanged ){
                        mdsTransUpdate.push({
                            id: foundMdsTrans.id, // ! must
                            id_community: this.instanceData.id_community,
                            id_user: matchedUser?matchedUser.id:userProfile?.id, // BG-1500
                            leaseid: j.unit_id,
                            mds_contract_record: j.tenant_id,
                            is_active: 1,
                            modified_date: currentDate 
                            // stop: 0
                        } as TMdsTransaction)
                    }
                }else{
                    // ! if not found, add & save  to mds transactions
                    mdsTransInsert.push({
                        id_community: this.instanceData.id_community,
                        id_user: matchedUser?matchedUser.id:userProfile?.id,
                        mds_contract_record: j.tenant_id,
                        is_active: 1,
                        created_date: currentDate ,
                        stop: 0
                    } as TMdsTransaction)
                }

            } // * for-of

            if( profilesToUpdate.length ){
                const profilesToUpdateCopy=profilesToUpdate.slice()
                // ! update profiles
                const profileModel = await this.dbTask.getModel(ModelType.Customer,"Profile",ProfilerAttributes,CustomerTable.Profiles)
                await this.dbTask.updateBulk(profileModel,profilesToUpdateCopy,['id_user','first_name','last_name','display_name','email','is_valid_email','is_local','is_acl_actived','phone','is_valid_home_phone','cell_phone','is_valid_cell_phone','is_deleted'])
            }

            const mdsTransModel = await this.dbTask.getModel(ModelType.Customer,'MdsTrans',MdsTransactionAttributes,CustomerTable.MdsTransactions)
            if( mdsTransInsert.length ){
                const mdsTransInsertReversed=mdsTransInsert.slice().reverse()
                // ! add to mds transactions                    
                await this.dbTask.saveBulk(mdsTransModel,mdsTransInsertReversed)
            }

            if( mdsTransUpdate.length ){
                const mdsTransUpdateCopy=mdsTransUpdate.slice()
                // ! update mds transactions                    
                await this.dbTask.updateBulk(mdsTransModel,mdsTransUpdateCopy,['id_community','id_user','modified_date'])
            }

            // ! get fresh data
            dbUserProfiles= await this.dbTask.getUserProfilesByAccountIDsOrEmails(jAccountIDs,allUniqJEmails)
            // ! iterate to create/update unit profiles
            for await(const j of jUsers){
                let userProfile = await (async()=>{
                        return dbUserProfiles.find(d=>d.user_email?.trim().toLowerCase()==j.email  && j.email || d.id_account==j.tenant_id && d.user_email?.trim().toLowerCase()==j.email && j.email || d.id_account==j.tenant_id)
                })()
                if( !userProfile ) {
                    continue
                }
                // !  check if already added in unit profiles
                let unitProfile = await( async()=>{
                    return dbUnitProfiles.find(d=>d.id_community==this.instanceData.id_community && userProfile && d.title==j.unit_title && d.id_user==userProfile.id)
                })()
                if( !unitProfile ){
                    unitProfilesCreateList.push({...userProfile,unit_title:j.unit_title,
                    resident_type: j.resident_type,
                    is_resident: j.is_resident??1
                    } as IDbUserUnit)
                }else{ // * found the unit profile record
                    if( unitProfile.id_resident_type!=DbTask.getResidentTypeID(j.resident_type) || 
                    unitProfile.id_group_type!=DbTask.getGroupTypeID(j.resident_type,j.is_resident??1) || 
                        unitProfile.is_resident!=(j.is_resident??1)  ){
                            unitProfilesUpdateList.push({
                                id: unitProfile.id,
                                id_community: unitProfile.id_community,
                                id_unit: unitProfile.id_unit,
                                id_user: unitProfile.id_user, // TODO: review
                                is_resident: j.is_resident??1,
                                id_group_type: DbTask.getGroupTypeID(j.resident_type,j.is_resident??1),
                                id_resident_type: DbTask.getResidentTypeID(j.resident_type)
                            } as TDbUnitProfile)

                            let oldProfileTypeID=Number(unitProfile.id_resident_type )
                            let newProfileTypeID = DbTask.getResidentTypeID(j.resident_type)??3
                            // ! if unit profile has changed by resident type
                            if( oldProfileTypeID!=newProfileTypeID ){ 

                                if( ownerGroupID ){
                                    if( ownerTypeIDs.includes(oldProfileTypeID) && newProfileTypeID==ProfileTypes.Tenant.id ) {
                                        // ! remove from owners
                                        roleGroupsToRemove.push({
                                            id_instance: this.instanceData.id,
                                            id_group: ownerGroupID,
                                            id_role: unitProfile.id_user
                                        })
                                    }
                                }

                                if( tenantGroupID ){
                                    // 3|1 = tenant to owner | 3|2 = tenant to owner
                                    if( oldProfileTypeID==ProfileTypes.Tenant.id && ownerTypeIDs.includes(newProfileTypeID) ) {
                                        // ! remove from tenant
                                        roleGroupsToRemove.push({
                                            id_instance: this.instanceData.id,
                                            id_group: tenantGroupID,
                                            id_role: unitProfile.id_user
                                        })
                                    }
                                }
                            }
                        }
                }
            } // * for-of
            
            if( roleGroupsToRemove.length ){
                await this.dbTask.hardDeleteRoleGroups(roleGroupsToRemove)
            }

            //  ! get fresh copy
            dbUserProfiles = await this.dbTask.getUserProfilesByAccountIDsOrEmails(jAccountIDs,allUniqJEmails)
            if( unitProfilesCreateList.length ){
                const dbUnits = await this.dbTask.getUnits()
                const unitProfilesToAdd:IDbUnitProfiles=[]
                // ! add to unit profiles if not found
                for await(const aUser of unitProfilesCreateList){
                    if( !aUser.unit_title ){
                        // ! skip if not a valid unit title
                        continue
                    }
                    // ? find the unit title of the new user
                    if( !aUser.id ){ // this is a new user
                        let matchedUser = await(async()=>{
                            return dbUserProfiles.find(d=>d.user_email?.trim().toLowerCase()==aUser.email?.trim().toLowerCase() || d.id_account==aUser.id_account && d.user_email?.trim().toLowerCase()==aUser.email?.trim().toLowerCase())
                        })()
                        if( !matchedUser ){
                            continue // ! user was not saved earlier
                        }
                        aUser.id=matchedUser.id
                    }
                    // ! find if the unit was already saved
                    let unitProfile = await(async()=>{
                        return dbUnitProfiles.find( d=>d.id_community==this.instanceData.id_community &&  d.id_user==aUser.id && aUser.unit_title && d.title==aUser.unit_title)
                    })()
                    if( unitProfile ) {
                        continue // ! unit was already assigned to user, skip
                    }
                    let unit=await(async()=>{
                        return dbUnits.find(d=>d.title==aUser.unit_title)
                    })()
                    if( !unit ) {
                        continue // ! unit not saved earlier, skip
                    }
                    // ! get resident_type from jUsers
                    let jUser = await(async()=>{
                        return jUsers.find(j=>j.unit_title==aUser.unit_title && j.tenant_id== aUser.id_account)
                    })()
                    // ! otherwise add user to unit if changed
                    unitProfilesToAdd.push({
                        id_community: this.instanceData.id_community,
                        id_unit:unit.id,
                        id_user:aUser.id,
                        is_resident:1,
                        id_group_type: DbTask.getGroupTypeID(aUser.resident_type,aUser.is_resident??1),
                        id_resident_type:  DbTask.getResidentTypeID(aUser.resident_type),
                        created_date:nowTimeStamp
                    } as TDbUnitProfile)
                } // * for-of

                if( unitProfilesToAdd.length ){
                    const unitProfilesToAddReversed=unitProfilesToAdd.slice().reverse()
                    const modelUnitProfiles = await this.dbTask.getModel(ModelType.Customer,'UnitProfile',UnitProfileAttributes,CustomerTable.UnitProfiles)
                    await this.dbTask.saveBulk(modelUnitProfiles,unitProfilesToAddReversed)
                }         
            }

            if( unitProfilesUpdateList.length ){
                const unitProfilesToUpdateList=unitProfilesUpdateList.slice()
                const modelUnitProfiles = await this.dbTask.getModel(ModelType.Customer,'UnitProfile',UnitProfileAttributes,CustomerTable.UnitProfiles)
                await this.dbTask.updateBulk(modelUnitProfiles,unitProfilesToUpdateList,['is_resident','id_group_type','id_resident_type'])
            }

            if( newAccountIDs.length ){
                // clear temporary reference
                await this.dbTask.clearAccountIDsFromUsers(newAccountIDs)
            }

            return{
                success:true,
                updateComplete:true
            }

        }catch(error){
            console.error(error)
            throw error
        }
    }

}

export default ResidentParser