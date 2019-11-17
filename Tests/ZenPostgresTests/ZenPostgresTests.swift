import XCTest
import NIO
import PostgresNIO

@testable import ZenPostgres


final class ZenPostgresTests: XCTestCase {
    
    private let config = PostgresConfig(
        host: "localhost",
        port: 5432,
        tls: false,
        username: "gerardo",
        password: "",
        database: "zenpostgres"
    )
    
    private var eventLoopGroup: EventLoopGroup!
    private var connection: PostgresConnection!
    private var pool: ZenPostgres!
    
    override func setUp() {
        do {
            eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
            pool = ZenPostgres(config: config, eventLoopGroup: eventLoopGroup)
            connection = try pool.connect().wait()
        } catch {
            XCTFail(error.localizedDescription)
        }
    }
    
    override func tearDown() {
        pool.disconnect(connection)
        try! eventLoopGroup.syncShutdownGracefully()
    }

    func testCreateTablesAsync() {
        Organization(connection: connection).create().whenComplete { result in
            switch result {
            case .success(_):
                XCTAssertTrue(true)
            case .failure(let err):
                XCTFail(err.localizedDescription)
            }
        }

        Account(connection: connection).create().whenComplete { result in
            switch result {
            case .success():
                XCTAssertTrue(true)
            case .failure(let err):
                XCTFail(err.localizedDescription)
            }
        }

        File(connection: connection).create().whenComplete { result in
            switch result {
            case .success():
                XCTAssertTrue(true)
            case .failure(let err):
                XCTFail(err.localizedDescription)
            }
        }
        
        sleep(2)
    }

    func testDropTablesAsync() {
        Account(connection: connection).drop().whenComplete { result in
            switch result {
            case .success(_):
                XCTAssertTrue(true)
            case .failure(let err):
                XCTFail(err.localizedDescription)
            }
        }

        Organization(connection: connection).drop().whenComplete { result in
            switch result {
            case .success(_):
                XCTAssertTrue(true)
            case .failure(let err):
                XCTFail(err.localizedDescription)
            }
        }

        File(connection: connection).drop().whenComplete { result in
            switch result {
            case .success(_):
                XCTAssertTrue(true)
            case .failure(let err):
                XCTFail(err.localizedDescription)
            }
        }

        sleep(2)
    }

    func testInsert() {
        for i in 0...100 {
            let organization = Organization(connection: connection)
            organization.organizationName = "Organization \(i)"
            let store = Store()
            store.storeId = i
            store.storeName = "Store \(i)"
            organization.organizationStore = store
            
            organization.save().whenSuccess { id in
                organization.organizationId = id as! Int
                XCTAssertTrue(organization.organizationId > 0)
                
                let account = Account(connection: self.connection)
                account.organizationId = organization.organizationId
                account.accountName = "Gerardo Grisolini"
                account.accountEmail = "gerardo@grisolini.com"
                account.save().whenComplete { _ in }
            }
        }
        sleep(5)
    }

    func testUpdate() {
        let organization = Organization(connection: connection)
        organization.organizationId = 1
        organization.organizationName = "Organization \(Date())"
        let store = Store()
        store.storeId = 1
        store.storeName = "Store 2"
        organization.organizationStore = store
        XCTAssertNoThrow(try organization.save().wait())
    }

    func testSelect() {
        let organization = Organization(connection: connection)
        XCTAssertNoThrow(try organization.get(1).wait())
        XCTAssertTrue(!organization.organizationName.isEmpty)
    }
    
    func testDelete() {
        XCTAssertNoThrow(try Organization(connection: connection).delete(1).wait())
    }

    func testQuerySelect() {
        let o = Organization(connection: connection)
        
        for _ in 0...100 {
            let sql = o.querySQL(
                columns: ["Organization.*"],
                whereclause: "Organization.organizationId > $1",
                params: [0],
                orderby: ["Organization.organizationName"],
                joins: [
                    DataSourceJoin(
                        table: "Account",
                        onCondition: "Organization.organizationId = Account.organizationId",
                        direction: .INNER)
                ]
            )
            o.sqlRowsAsync(sql).whenComplete { result in
                switch result {
                case .success(let rows):
                    XCTAssertTrue(rows.count > 0)
                case .failure(let error):
                    XCTFail(error.localizedDescription)
                }
            }
        }
        
        sleep(3)
    }

    func testQueryUpdate() {
        for _ in 0...100 {
            Account(connection: connection).update(
                cols: ["accountEmail"],
                params: ["gg@grisolini.com"],
                id: "accountName",
                value: "Gerardo Grisolini"
            ).whenComplete { result in
                switch result {
                case .success(let count):
                    XCTAssertTrue(count > 0)
                case .failure(let error):
                    XCTFail(error.localizedDescription)
                }
            }
        }
        sleep(3)
    }
    
    func testQueryDelete() {
        Account(connection: connection!).delete(
            key: "accountEmail",
            value: "gg@grisolini.com"
        ).whenComplete { result in
            switch result {
            case .success(let count):
                XCTAssertTrue(count > 0)
            case .failure(let error):
                XCTFail(error.localizedDescription)
            }
        }
        sleep(3)
    }
    
    func testInsertFile() {
        let file = File(connection: connection)
        //XCTAssertNoThrow(try file.create())

        file.name = "IMG_0001.png"
        file.contentType = "image/png"
        if let data = FileManager.default.contents(atPath: "/Users/gerardo/Downloads/IMG_0001.png") {
            file.data = [UInt8](data)
        }
        XCTAssertNoThrow(try file.save().wait())
        XCTAssertTrue(file.id > 0)
    }

    func testGetFile() {
        let file = File(connection: connection)
        //XCTAssertNoThrow(try file.get(5))
        file.get(1).whenComplete { result in
            switch result {
            case .success(_):
                print(file.name)
                XCTAssertTrue(file.data.count > 0)
            case .failure(let err):
                XCTFail(err.localizedDescription)
            }
        }
        
        sleep(3)
    }

    static var allTests = [
        ("testCreateTablesAsync", testCreateTablesAsync),
        ("testDropTablesAsync", testDropTablesAsync),
        ("testInsert", testInsert),
        ("testSelect", testSelect),
        ("testUpdate", testUpdate),
        ("testDelete", testDelete),
        ("testQuerySelect", testQuerySelect),
        ("testQueryUpdate", testQueryUpdate),
        ("testQueryDelete", testQueryDelete),
        ("testInsertFile", testInsertFile),
        ("testGetFile", testGetFile)
    ]

    class Store: PostgresJson {
        public var storeId: Int = 0
        public var storeName: String = ""
    }
    
    class Organization: PostgresTable, Codable {
        public var organizationId: Int = 0
        public var organizationName: String = ""
        public var organizationStore: Store = Store()
        public var _account: Account = Account()
        
        enum CodingKeys: String, CodingKey {
            case organizationId = "organizationId"
            case organizationName = "organizationName"
            case organizationStore = "organizationStore"
        }
    
//        required init() {
//            super.init()
//        }
//
//        required init(from decoder: Decoder) throws {
//            super.init()
//
//            let container = try decoder.container(keyedBy: CodingKeys.self)
//            organizationId = try container.decode(Int.self, forKey: .organizationId)
//            organizationName = try container.decode(String.self, forKey: .organizationName)
//            organizationStore = try container.decode(Store.self, forKey: .organizationStore)
//        }
//
//        func encode(to encoder: Encoder) throws {
//            var container = encoder.container(keyedBy: CodingKeys.self)
//            try container.encode(organizationId, forKey: .organizationId)
//            try container.encode(organizationName, forKey: .organizationName)
//            try container.encode(organizationStore, forKey: .organizationStore)
//        }

        override func decode(row: PostgresRow) {
            organizationId = row.column("organizationId")?.int ?? organizationId
            organizationName = row.column("organizationName")?.string ?? organizationName
            organizationStore = try! row.column("organizationStore")?.jsonb(as: Store.self) ?? organizationStore
            _account.decode(row: row)
        }
    }
    
    class Account: PostgresTable, Codable {
        public var accountId: Int = 0
        public var organizationId: Int = 0
        public var accountName: String = ""
        public var accountEmail: String = ""
        
        required init() {
            super.init()
            self.tableIndexes.append("accountEmail")
        }
        
        override func decode(row: PostgresRow) {
            accountId = row.column("accountId")?.int ?? accountId
            organizationId = row.column("organizationId")?.int ?? organizationId
            accountName = row.column("accountName")?.string ?? accountName
            accountEmail = row.column("accountEmail")?.string ?? accountEmail
        }
    }
    
    class File: PostgresTable, Codable {
        public var id: Int = 0
        public var name: String = ""
        public var data: [UInt8] = [UInt8]()
        public var contentType: String = ""
        
        override func decode(row: PostgresRow) {
            id = row.column("id")?.int ?? id
            name = row.column("name")?.string ?? name
            data = row.column("data")?.bytes ?? data
            contentType = row.column("contentType")?.string ?? contentType
        }
        
        override func save() -> EventLoopFuture<Any> {
            let text = """
INSERT INTO "File" ("name", "data", "contentType") VALUES ($1, $2, $3) RETURNING id
"""
            let postgresData = [
                PostgresData(string: name),
                PostgresData(bytes: data),
                PostgresData(string: contentType)
            ]
            
            return connection!.query(text, postgresData).map { rows -> Any in
                self.id = rows.first?.column("id")?.int ?? self.id
                return self.id
            }
        }
    }
}
