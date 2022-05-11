// Quick start, create an active ftp server.
const { FtpSrv, FileSystem } = require('ftp-srv');
const errors = require('ftp-srv/src/errors');
const _ = require('lodash');
const nodePath = require('path');
const uuid = require('uuid');
const Promise = require('bluebird');
const fsAsync = require('ftp-srv/src//helpers/fs-async');


const { Writable } = require('stream');
const { Buffer } = require('buffer');
Buffer.poolSize = 1024 * 1024;

const { networkInterfaces } = require('os');
const { Netmask } = require('netmask');

const { Kafka } = require('kafkajs')

const yargs = require('yargs');
const path = require('path');
require('dotenv').config();

const { authenticate } = require('ldap-authentication')

const args = setupYargs();
const state = setupState(args);

let ldap_client = null;

function setupYargs() {
    return yargs
        .option('credentials', {
            alias: 'c',
            describe: 'Load user & pass from json file',
            normalize: true,
            default: process.env.KAFKATP_CREDENTIALS || null
        })
        .option('username', {
            describe: 'Blank for anonymous',
            type: 'string',
            default: process.env.KAFKATP_USERNAME || null
        })
        .option('password', {
            describe: 'Password for given username',
            type: 'string',
            default: process.env.KAFKATP_PASSWORD || null
        })
        .option('ldapserver', {
            describe: 'LDAP server URL',
            type: 'string',
            default: process.env.KAFKATP_LDAPSERVER || null
        })
        .option('ldapbinddn', {
            describe: 'LDAP server bind dn',
            type: 'string',
            default: process.env.KAFKATP_LDAPBINDDN || null
        })
        .option('ldapbindpass', {
            describe: 'LDAP server bind password',
            type: 'string',
            default: process.env.KAFKATP_LDAPBINDPASS || null
        })
        .option('ldapsearch', {
            describe: 'LDAP domain search',
            type: 'string',
            default: process.env.KAFKATP_LDAPSEARCH || null
        })
        .option('root', {
            alias: 'r',
            describe: 'Default root directory for users',
            type: 'string',
            normalize: true,
            default: process.env.KAFKATP_ROOT || ''
        })
        .option('read-only', {
            describe: 'Disable write actions such as upload, delete, etc',
            boolean: true,
            default: (process.env.KAFKATP_ROOT || 'FALSE') === 'TRUE'
        })
        .option('anonymous', {
            describe: 'Enable anonymous login',
            boolean: true,
            default: (process.env.KAFKATP_ROOT || 'FALSE') === 'TRUE'
        })
        .option('url', {
            describe: 'URL',
            type: 'string',
            default: process.env.KAFKATP_URL || 'ftp://0.0.0.0:21'
        })
        .option('pasv-url', {
            describe: 'URL to provide for passive connections',
            type: 'string',
            alias: 'pasv_url',
            default: process.env.KAFKATP_PASVURL || 'localhost'
        })
        .option('pasv-min', {
            describe: 'Starting point to use when creating passive connections',
            type: 'number',
            default: Number(process.env.KAFKATP_PASVMIN || '1024'),
            alias: 'pasv_min'
        })
        .option('pasv-max', {
            describe: 'Ending port to use when creating passive connections',
            type: 'number',
            default: Number(process.env.KAFKATP_PASVMAX || '65535'),
            alias: 'pasv_max'
        })
        .option('kafka-url', {
            describe: 'URL to provide for kafka connections',
            type: 'string',
            alias: 'kafka_url',
            default: process.env.KAFKATP_KAFKAURL || 'localhost'
        })
        .option('kafka-port', {
            describe: 'Port to provide for kafka connections',
            type: 'string',
            alias: 'kafka_port',
            default: process.env.KAFKATP_KAFKAPORT || '9092'
        })
        .option('kafka-topic', {
            describe: 'Kafka publishing topic',
            type: 'string',
            alias: 'kafka_topic',
            default: process.env.KAFKATP_KAFKATOPIC || 'kafkatp'
        })
        .parse();
}

function setupState(_args) {
    const _state = {};

    function setupOptions() {

        _state.url = _args.url;
        _state.pasv_url = _args.pasv_url;
        _state.pasv_min = _args.pasv_min;
        _state.pasv_max = _args.pasv_max;
        _state.anonymous = _args.anonymous;
        _state.kafka_url = _args.kafka_url;
        _state.kafka_port = _args.kafka_port;
        _state.kafka_topic = _args.kafka_topic;
    }

    function setupRoot() {
        const dirPath = _args.root;
        if (dirPath) {
            _state.root = dirPath;
        } else {
            _state.root = process.cwd();
        }
    }

    function setupCredentials() {
        _state.credentials = {};

        const setCredentials = (username, password, root = null) => {
            _state.credentials[username] = {
                password,
                root
            };
        };

        if (_args.credentials) {
            const credentialsFile = path.resolve(_args.credentials);
            const credentials = require(credentialsFile);

            for (const cred of credentials) {
                setCredentials(cred.username, cred.password, cred.root);
            }
        } else if (_args.username) {
            setCredentials(_args.username, _args.password);
        }
    }

    function setupCommandBlacklist() {
        if (_args.readOnly) {
            _state.blacklist = ['ALLO', 'APPE', 'DELE', 'MKD', 'RMD', 'RNRF', 'RNTO', 'STOR', 'STRU'];
        }
    }

    function setupLdapClient() {
        _state.ldapserver = _args.ldapserver
        _state.ldapbinddn = _args.ldapbinddn
        _state.ldapbindpass = _args.ldapbindpass
        _state.ldapsearch = _args.ldapsearch



    }

    setupOptions();
    setupRoot();
    if (_args.username || args.credentials) {
        console.log('Setting up static credentials!')
        setupCredentials();
    }
    else if (_args.ldapserver) {
        console.log('Setting up LDAP client!')
        setupLdapClient();
    }
    else {
        console.log('No valid auth scheme provided!')
        process.exit(-1)
    }

    setupCommandBlacklist();

    return _state;
}


const kafka = new Kafka({
    clientId: 'kafkatp',
    brokers: [`${state.kafka_url}:${state.kafka_port}`]
})

const nets = networkInterfaces();
function getNetworks() {
    let networks = {};
    for (const name of Object.keys(nets)) {
        for (const net of nets[name]) {
            if (net.family === 'IPv4' && !net.internal) {
                networks[net.address + "/24"] = net.address
            }
        }
    }
    console.log(networks);
    return networks;
}

const resolverFunction = (address) => {
    // const networks = {
    //     '$GATEWAY_IP/32': `${public_ip}`, 
    //     '10.0.0.0/8'    : `${lan_ip}`
    // } 
    const networks = getNetworks();
    for (const network in networks) {
        if (new Netmask(network).contains(address)) {
            console.log('NNNNNNNN: ', networks[network]);
            return networks[network];
        }
    }
    return "127.0.0.1";
}

const UNIX_SEP_REGEX = /\//g;
const WIN_SEP_REGEX = /\\/g;

const ftpServer = new FtpSrv({
    url: state.url,
    pasv_url: state.pasv_url,
    pasv_min: state.pasv_min,
    pasv_max: state.pasv_max,
    anonymous: state.anonymous,
    blacklist: state.blacklist
});


const fs = require('fs');
const { exit } = require('process');

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

let connected = false;
let admin_connected = false;
const producer = kafka.producer();
const admin = kafka.admin();

(async () => {
    console.log('Connecting producer...');
    await producer.connect();
    connected = true;
    console.log('Producer connected: ', producer);
})();

(async () => {
    console.log('Connecting admin...');
    await admin.connect();

    admin_connected = true;
    console.log('Admin connected: ', admin);
    const topics = await admin.listTopics();
    console.log('Topics: ', topics);

})();

class WriteStream extends Writable {
    constructor(filename) {
        super();
        this.filename = filename;

        this.buf = Buffer.alloc(0);


    }
    _construct(callback) {
        callback();
    }
    _write(chunk, encoding, callback) {
        //console.log('CHUNK!', chunk.length, chunk)
        this.buf = Buffer.concat([this.buf, chunk])
        callback();
    }

    _final(callback) {
        console.log('FINAL!', this.filename, this.buf.length)
        callback();
        if (connected) {
            (async () => {
                console.log('Publishing...');
                await producer.send({
                    topic: state.kafka_topic,
                    messages: [
                        {
                            key: this.filename,
                            value: this.buf.toString('base64')
                        },
                    ],
                })
                console.log('published!');
            })();
        }
    }


    _destroy(err, callback) {
        callback();
    }
}


class KafkaFS extends FileSystem {
    constructor(connection, { root, cwd } = {}) {
        super(...arguments);

    }

    get root() {
        return this._root;
    }

    _resolvePath(path = '.') {
        // Unix separators normalize nicer on both unix and win platforms
        const resolvedPath = path.replace(WIN_SEP_REGEX, '/');

        // Join cwd with new path
        const joinedPath = nodePath.isAbsolute(resolvedPath)
            ? nodePath.normalize(resolvedPath)
            : nodePath.join('/', this.cwd, resolvedPath);

        // Create local filesystem path using the platform separator
        const fsPath = nodePath.resolve(nodePath.join(this.root, joinedPath)
            .replace(UNIX_SEP_REGEX, nodePath.sep)
            .replace(WIN_SEP_REGEX, nodePath.sep));

        // Create FTP client path using unix separator
        const clientPath = joinedPath.replace(WIN_SEP_REGEX, '/');

        return {
            clientPath,
            fsPath
        };
    }

    currentDirectory() {
        return this.cwd;
    }

    get(fileName) {
        console.log('get')
        const { fsPath } = this._resolvePath(fileName);
        return fsAsync.stat(fsPath)
            .then((stat) => _.set(stat, 'name', fileName));
    }

    list(path = '.') {
        console.log('list')
        return [];
    }

    chdir(path = '.') {
        const { fsPath, clientPath } = this._resolvePath(path);
        console.log('chdir')
        this.cwd = clientPath;
        return this.currentDirectory()
    }

    write(fileName, { append = false, start = undefined } = {}) {

        const { fsPath, clientPath } = this._resolvePath(fileName);
        const stream = new WriteStream(this.cwd + "/" + fileName)//createWriteStream('/dev/null', { flags: !append ? 'w+' : 'a+', start });
        stream.once('error', () => stream.end());
        stream.once('close', () => stream.end());
        //const stream = process.stdout;
        return {
            stream,
            clientPath
        };
    }

    read(fileName, { start = undefined } = {}) {
        console.log('read')
        throw new errors.FileSystemError('Cannot read!');
    }

    delete(path) {
        console.log('delete')
        throw new errors.FileSystemError('Cannot delete!');
    }

    mkdir(path) {
        console.log('mkdir')
        throw new errors.FileSystemError('Cannot create directories!');
    }

    rename(from, to) {
        console.log('rename')
        throw new errors.FileSystemError('Cannot Rename!');
    }

    chmod(path, mode) {
        console.log('chmod')
        throw new errors.FileSystemError('Cannot chmod!');
    }

    getUniqueName() {
        return uuid.v4().replace(/\W/g, '');
    }
}

function checkLogin(data, resolve, reject) {
    const user = state.credentials[data.username];
    if (state.anonymous || user && user.password === data.password) {
        const mfs = new KafkaFS(data.connection, '/', '/');
        return resolve({ root: user && user.root || state.root, fs: mfs });
    }

    return reject(new errors.GeneralError('Invalid username or password', 401));
}


async function checkLdapLogin(data, resolve, reject) {

    const mfs = new KafkaFS(data.connection, '/', '/');


    let options = {
        ldapOpts: {
            url: state.ldapserver,
            // tlsOptions: { rejectUnauthorized: false }
        },
        adminDn: state.ldapbinddn,
        adminPassword: state.ldapbindpass,
        userPassword: data.password,
        userSearchBase: state.ldapsearch,
        usernameAttribute: 'uid',
        username: data.username,
        // starttls: false
    };

    console.log('OPTIONS: ', options)

    try {
        const user = await authenticate(options)
        return resolve({ root: '/', fs: mfs });

    }
    catch (err) {
        return reject(new errors.GeneralError('Invalid username or password', 401));
    }

}



ftpServer.on('login', state.ldapserver ? checkLdapLogin : checkLogin);

ftpServer.listen().then(() => {
    console.log('Ftp server is starting...')
});
