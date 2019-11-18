//
//  PostgresTable.swift
//  ZenPostgres
//
//  Created by Gerardo Grisolini on 17/03/2019.
//

import Foundation
import PostgresNIO
import PostgresKit

/*
protocol KeyPathListable {
    associatedtype AnyOldObject
    // require empty init as the implementation use the mirroring API, which require
    // to be used on an instance. So we need to be able to create a new instance of the
    // type. See @@@^^^@@@
    init()

    var keyPathReadableFormat: [String: Any] { get }
    var allKeyPaths: [String:KeyPath<AnyOldObject, Any?>] { get }
}

extension KeyPathListable {
    var keyPathReadableFormat: [String: Any] {
        var description: [String: Any] = [:]
        let mirror = Mirror(reflecting: self)
        for case let (label?, value) in mirror.children {
            description[label] = value
        }
        return description
    }

    var allKeyPaths: [String:KeyPath<Self, Any?>] {
        var membersTokeyPaths: [String:KeyPath<Self, Any?>] = [:]
        let instance = Self()
        for (key, _) in instance.keyPathReadableFormat {
            membersTokeyPaths[key] = \Self.keyPathReadableFormat[key]
        }
        return membersTokeyPaths
    }
}
*/

open class PostgresTable {
    public var table: String = ""
    public var tableIndexes = [String]()
    private var pKey: String = "id"
    public var connection: PostgresConnection? = nil
    
    public convenience init(connection: PostgresConnection) {
        self.init()
        self.connection = connection
    }
    
    public required init() {
        table = String(describing: self)//.lowercased()
        if let index = table.lastIndex(where: { $0 == "." }) {
            table = table[table.index(after: index)...].description
        }
        
        if let first = Mirror(reflecting: self).children.first, let key = first.label {
            pKey = key//.lowercased()
        }
    }
    
    deinit {
        connection = nil
    }
    
    private var pValue: Int {
        return Mirror(reflecting: self).children.first?.value as? Int ?? 0
    }

    open func decode(row: PostgresRow) {
//        for child in self.allKeyPaths {
//            let key = child.key
//
//            print("member: ", key, " / value: ", self[keyPath: child.value])

//            guard !key.hasPrefix("_") else {
//                continue
//            }
//
//            guard let columnKey = row.column(key) else {
//                continue
//            }

            
//            switch child.value {
//            case is Bool:
//                child.value = columnKey.bool!
//            case is Float:
//                child.value = columnKey.float!
//            case is Double:
//                child.value = columnKey.double!
//            case is Int:
//                child.value = columnKey.int!
//            case is Int16:
//                child.value = columnKey.int16!
//            case is Int32:
//                child.value = columnKey.int32!
//            case is Int64:
//                child.value = columnKey.int64!
//            case is Date:
//                child.value = columnKey.date!
//            case is String:
//                child.value = columnKey.string!
//            case is UUID:
//                child.value = columnKey.uuid!
//            case is PostgresJson:
//                let obj = child.value as! PostgresJson
//                obj.decode(data: columnKey.jsonb!)
//                child.value = obj
//            default:
//                break
//            }

//        }
    }
    
    fileprivate func query(_ conn: PostgresConnection, _ sql: String) -> EventLoopFuture<[PostgresRow]> {
        ZenPostgres.pool.log("🔄 \(sql)")
        return conn.query(sql)
    }
    
    public func sqlRowsAsync(_ sql: String) -> EventLoopFuture<[PostgresRow]> {
        if let conn = connection {
            return query(conn, sql)
        } else {
            return ZenPostgres.pool.connect().flatMap { conn -> EventLoopFuture<[PostgresRow]> in
                defer { conn.disconnect() }
                return self.query(conn, sql)
            }
        }
    }

    fileprivate var createSQL: String {
        var opt = [String]()
        var keyName = ""
        for child in Mirror(reflecting: self).children {
            guard let key = child.label else {
                continue
            }
            var verbage = ""
            if !key.hasPrefix("_") {
                verbage = "\"\(key)\" "
                if child.value is Int && opt.count == 0 {
                    verbage += "serial"
                } else if child.value is Int {
                    verbage += "int DEFAULT 0"
                    if key.hasSuffix("Id") {
                        let table = key[..<key.index(key.endIndex, offsetBy: -2)].description
                        verbage += " REFERENCES \"\(table.capitalizingFirstLetter())\" ON DELETE CASCADE"
                    }
                } else if child.value is Bool {
                    verbage += "boolean DEFAULT false"
                } else if child.value is Character {
                    verbage += "char DEFAULT ' '"
                } else if child.value is Double {
                    verbage += "double precision DEFAULT 0"
                } else if child.value is Int64 {
                    verbage += "bigint DEFAULT 0"
                } else if child.value is [UInt8] {
                    verbage += "bytea"
                } else if child.value is PostgresJson || if child.value is Data {
                    verbage += "jsonb"
                } else if child.value is String && key.contains("xml") {
                    verbage += "xml"
                } else {
                    verbage += "text"
                }
                if opt.count == 0 {
                    verbage += " NOT NULL"
                    keyName = key
                }
                opt.append(verbage)
            }
        }
        let keyComponent = ", CONSTRAINT \(table)_key PRIMARY KEY (\"\(keyName)\") NOT DEFERRABLE INITIALLY IMMEDIATE"
        return "CREATE TABLE IF NOT EXISTS \"\(table)\" (\(opt.joined(separator: ", "))\(keyComponent))"
    }
    
    fileprivate var createIndexes: [String] {
        return tableIndexes.map { index -> String in
            return "CREATE UNIQUE INDEX IF NOT EXISTS \(index)_idx ON \"\(table)\"(\"\(index)\")"
        }
    }
    
    
    // Sql
    
    fileprivate var dropSQL: String {
        return "DROP TABLE \"\(table)\""
    }

    fileprivate var selectSQL: String {
        return "SELECT * FROM \"\(table)\""
    }

    fileprivate var insertSQL: String {
        var cols = [String]()
        var values = [String]()
        let childrens = Mirror(reflecting: self).children
        for children in childrens.dropFirst() {
            guard let key = children.label, !key.hasPrefix("_") else { continue }
            cols.append("\"\(key)\"")
            values.append("\(parseValue(children.value))")
        }
        return "INSERT INTO \"\(table)\" (\(cols.joined(separator: ", "))) VALUES (\(values.joined(separator: ","))) RETURNING \"\(pKey)\""
    }
    
    fileprivate var updateSQL: String {
        var cols = [String]()
        let childrens = Mirror(reflecting: self).children.dropFirst()
        for children in childrens {
            guard let key = children.label, !key.hasPrefix("_") else { continue }
            cols.append("\"\(key)\" = \(parseValue(children.value))")
        }
        return "UPDATE \"\(table)\" SET \(cols.joined(separator: ", ")) WHERE \"\(pKey)\" = \(pValue) RETURNING \"\(pKey)\""
    }
    
    fileprivate var deleteSQL: String {
        return "DELETE FROM \"\(table)\" WHERE \"\(pKey)\" = \(pValue) RETURNING 1"
    }
    
    
    /// Create
    
    open func create() -> EventLoopFuture<Void> {
        return sqlRowsAsync(createSQL).map { rows -> Void in
            ()
        }
    }
    

    /// Drop

    open func drop() -> EventLoopFuture<Void> {
        sqlRowsAsync(dropSQL).map { rows -> Void in
            ()
        }
    }

    
    /// Save

    open func save() -> EventLoopFuture<Any> {
        let sql = pValue == 0 ? insertSQL : updateSQL
        return sqlRowsAsync(sql)
            .flatMapThrowing { rows -> Any in
                if let id = rows.first?.column(self.pKey)?.int {
                    return id
                }
                throw ZenError.recordNotSave
            }
    }

    
    // Get
    
    open func get(_ id: Int) -> EventLoopFuture<Void> {
        return get(pKey, id)
    }
    
    open func get(_ key: String, _ value: Any) -> EventLoopFuture<Void> {
        let sql = "\(selectSQL) WHERE \"\(key.isEmpty ? pKey : key)\" = '\(value)' LIMIT 1 OFFSET 0"
                
        return sqlRowsAsync(sql).flatMapThrowing { rows -> Void in
            if let row = rows.first {
                return self.decode(row: row)
            } else {
                throw ZenError.recordNotFound
            }
        }
    }
    

    // Query

    public func querySQL(
        columns:        [String] = [],
        whereclause:    String = "",
        params:         [Any] = [],
        orderby:        [String] = [],
        cursor:         Cursor = Cursor(limit: 0, offset: 0),
        joins:          [DataSourceJoin] = [],
        having:         [String] = [],
        groupBy:        [String] = []) -> String {
        
        var clauseSelect = ""
        var clauseWhere = ""
        var clauseOrder = ""
        var clauseJoin = ""
        
        var keys = [String]()
        keys.append(joins.count == 0 ? "*" : "\"\(table)\".*")
        for join in joins {
            keys.append("\"\(join.table)\".*")
            clauseJoin += " \(join.direction) JOIN \"\(join.table)\""
            let splitted = join.onCondition.split(separator: " ").map { str -> String in
                str.contains(".") ? "\"\(str.replacingOccurrences(of: ".", with: "\".\""))\"" : str.description
            }
            clauseJoin += " ON \(splitted.joined(separator: " "))"
        }
        
        if columns.isEmpty {
            clauseSelect = keys.joined(separator: ", ")
        } else {
            clauseSelect = columns.map({ str -> String in
                var array = str.split(separator: " ")
                let index = array.count - 1
                array[index] = array[index].contains(".*")
                    ? "\"\(array[index].replacingOccurrences(of: ".*", with: "\".*"))"
                    : "\"\(str.replacingOccurrences(of: ".", with: "\".\""))\""
                return array.joined(separator: " ")
            }).joined(separator: ", ")
        }
        
        if !whereclause.isEmpty {
            clauseWhere = whereclause
                .split(separator: " ")
                .map({ $0.contains(".") ? "\"\($0.replacingOccurrences(of: ".", with: "\".\""))\"" : $0.count > 3 ? "\"\($0)\"" : $0 })
                .joined(separator: " ")
            clauseWhere = " WHERE \(clauseWhere)"
        }
        
        for i in 0..<params.count {
            //let replace = params[i] is Int ? "\(params[i])" : "'\(params[i])'"
            var replace = "\(params[i])"
            if !replace.hasSuffix("jsonb") {
                replace = "'\(replace)'"
            }
            clauseWhere = clauseWhere.replacingOccurrences(of: "$\(i+1)", with: replace)
        }
        
        if orderby.count > 0 {
            let colsjoined = orderby.map({ str -> String in
                var array = str.split(separator: " ")
                array[0] = "\"\(array[0].replacingOccurrences(of: ".", with: "\".\""))\""
                return array.joined(separator: " ")
            }).joined(separator: ",")
            clauseOrder = " ORDER BY \(colsjoined)"
        }
        
        var sql = "SELECT \(clauseSelect) FROM \"\(table)\"\(clauseJoin)\(clauseWhere)\(clauseOrder)"
        
        if cursor.limit > 0 {
            sql += " LIMIT \(cursor.limit)"
        }
        if cursor.offset > 0 {
            sql += " OFFSET \(cursor.offset)"
        }
        
        return sql
    }
    
    public func query<T: PostgresTable>(sql: String) -> EventLoopFuture<[T]> {
        return sqlRowsAsync(sql).map { rows -> [T] in
            return rows.map { row -> T in
                let r = T()
                r.decode(row: row)
                return r
            }
        }
    }
    
    public func query<T: PostgresTable>(
        columns:        [String] = [],
        whereclause:    String = "",
        params:         [Any] = [],
        orderby:        [String] = [],
        cursor:         Cursor = Cursor(limit: 0, offset: 0),
        joins:          [DataSourceJoin] = [],
        having:         [String] = [],
        groupBy:        [String] = []) -> EventLoopFuture<[T]> {
        
        let sql = self.querySQL(
            columns: columns,
            whereclause: whereclause,
            params: params,
            orderby: orderby,
            cursor: cursor,
            joins: joins,
            having: having,
            groupBy: groupBy
        )
        
        return query(sql: sql)
    }

    
    // Update

    public func update(cols: [String], params: [Any], id: String, value: Any) -> EventLoopFuture<Int> {
        var set = [String]()
        for i in 0..<params.count {
            set.append("\"\(cols[i])\" = \(parseValue(params[i]))")
        }
        let sql = "UPDATE \"\(table)\" SET \(set.joined(separator: ", ")) WHERE \"\(id)\" = '\(value)' RETURNING 1"
        
        return sqlRowsAsync(sql).map { rows -> Int in
            rows.count
        }
    }


    // Delete
    
    public func delete() -> EventLoopFuture<Bool> {
        return sqlRowsAsync(deleteSQL).map { rows -> Bool in
            rows.count > 0
        }
    }

    public func delete(_ id: Int) -> EventLoopFuture<Bool> {
        return delete(key: pKey, value: id).map { count -> Bool in
            count > 0
        }
    }
        
    public func delete(key: String, value: Any) -> EventLoopFuture<Int> {
        let sql = "DELETE FROM \"\(table)\" WHERE \"\(key)\" = '\(value)' RETURNING 1"
        return sqlRowsAsync(sql).map { rows -> Int in
            rows.count
        }
    }

    
    // Utils
    
    fileprivate func cast<A>(value: Any, type: A.Type) -> A? {
        return value as? A
    }

    fileprivate func parseValue(_ value: Any) -> String {
        if let obj = cast(value: value, type: Int.self) {
            return "\(obj)"
        } else if let obj = cast(value: value, type: PostgresJson.self) {
            return "\(obj)"
        } else if let obj = cast(value: value, type: [PostgresJson].self) {
            return "\(obj)"
        } else if let obj = cast(value: value, type: Optional<PostgresJson>.self) {
            if let obj = obj {
                return "\(obj)"
            } else {
                return "NULL"
            }
        } else {
            let value = "\(value)".replacingOccurrences(of: "'", with: "''")
            return value == "nil" ? "NULL" : "'\(value)'"
        }
    }
}
    
public protocol PostgresJson: Codable {
    var json: String { get }
}

extension PostgresJson {
    public var json: String {
        let json = try! JSONEncoder().encode(self)
        return String(data: json, encoding: .utf8)!//.replacingOccurrences(of: "'", with: "''")
    }
}

extension String.StringInterpolation {
    mutating func appendInterpolation(_ value: PostgresJson) {
        appendInterpolation("'\(value.json)'::jsonb")
    }

    mutating func appendInterpolation(_ value: [PostgresJson]) {
        let json = value.map { $0.json }.joined(separator: ",")
        appendInterpolation("'[\(json)]'::jsonb")
    }

    mutating func appendInterpolation(_ value: Optional<PostgresJson>) {
        appendInterpolation("'\(value!.json)'::jsonb")
    }
}

