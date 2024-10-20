import { parseStringPromise } from 'xml2js';
import fs from 'fs';
import path from 'path';

class Typebatis
{
  private DynamicQueries: Map<string, any> = new Map();

  constructor()
  {
    const configPath = path.resolve(process.cwd(), '.typebatis.json');

    if (fs.existsSync(configPath))
    {
      const configContent = fs.readFileSync(configPath, 'utf-8');
      const json = JSON.parse(configContent);
      const dir = path.resolve(process.cwd(), json.xml);

      this.loadXmlFiles(dir);
    }
    else
    {
      console.error("Typebatis: .typebatis.json not found");
    }
  }

  private loadXmlFiles(_path: string)
  {
    const files = fs.readdirSync(_path);

    files.forEach((file) =>
    {
      const fullPath = path.join(_path, file);
      const stats = fs.statSync(fullPath);

      if (stats.isDirectory())
      {
        this.loadXmlFiles(fullPath);
      }
      else if (file.endsWith('.xml'))
      {
        const xmlContent = fs.readFileSync(fullPath, 'utf-8');

        this.loadXmlQueries(xmlContent);
      }
    });
  }

  private async loadXmlQueries(_content: string)
  {
    try
    {
      const xml = await parseStringPromise(_content);
      const namespace = xml.mapper?.$?.namespace;

      if (namespace)
      {
        const queries = xml.mapper;

        Object.keys(queries).forEach(id =>
        {
          if (id !== '$' && queries[id])
          {
            const uniqueKey = `${namespace}.${id}`;
            this.DynamicQueries.set(uniqueKey, queries[id]);
          }
        });
      }
    } catch (error)
    {
      console.error("Typebatis: failed parse XML content", error);
    }
  }

  public async executeQuery(_id: string, _params: any): Promise<any>
  {
    return new Promise<any>((resolve, reject) =>
    {
      const query = this.getQuery(_id);
      const sql = this.generateSql(query, _params);

      if (query)
      {
        resolve(sql);
      }
      else
      {
        reject("Typebatis: failed to generate query - ${id}");
      }

    });
  }

  public getQuery(_id: string): any
  {
    if (this.DynamicQueries.has(_id))
    {
      return this.DynamicQueries.get(_id);
    }

    throw new Error(`Typebatis: ${_id} not found`);
  }

  private generateSql(_query: any, _params: any): string
  {
    let sql = _query._;

    /**
     * <if>
     * 
     */
    if (_query.if)
    {
      _query.if.forEach((con: any) =>
      {
        const condition = con.$.test;
        const content = con._;

        if (this.evaluateCondition(condition, _params))
        {
          sql += content.trim();
        }
      });
    }

    /**
     * <choose> <when> <otherwise>
     * 
     */
    if (_query.choose)
    {
      const condition = _query.choose[0];
      let chosen = false;

      if (condition.when)
      {
        condition.when.forEach((con: any) =>
        {
          if (!chosen && this.evaluateCondition(con.$.test, _params))
          {
            sql += con._.trim();
            chosen = true;
          }
        });
      }

      if (!chosen && condition.otherwise)
      {
        sql += condition.otherwise[0]._.trim();
      }
    }

    /**
     * <set>
     * 
     */
    if (_query.set)
    {
      let setContent = '';

      _query.set.forEach((con: any) =>
      {
        con.if.forEach((con_if: any) =>
        {
          if (this.evaluateCondition(con_if.$.test, _params))
          {
            setContent += con_if._.trim() + ', ';
          }
        });
      });

      if (setContent.length > 0)
      {
        sql += ` SET ${setContent.slice(0, -2)}`;
      }
    }

    /**
     * <foreach>
     * 
     */
    if (_query.foreach)
    {
      const foreachCondition = _query.foreach[0];
      const collection = foreachCondition.$.collection;
      const item = foreachCondition.$.item;
      const open = foreachCondition.$.open || '';
      const close = foreachCondition.$.close || '';
      const separator = foreachCondition.$.separator || ',';
      const collectionValue = this.getNestedValue(_params, collection);

      if (Array.isArray(collectionValue))
      {
        const result = collectionValue.map((v: any) =>
        {
          let foreachSql = foreachCondition._.replace(new RegExp(`#{${item}}`, 'g'), v);

          if (foreachCondition.if)
          {
            foreachCondition.if.forEach((con: any) =>
            {
              if (this.evaluateCondition(con.$.test, { ..._params, [item]: v }))
              {
                foreachSql += con._.trim();
              }
            });
          }

          return foreachSql;

        }).join(separator);

        sql += `${open}${result}${close}`;
      }
    }

    /**
     * <where>
     * 
     */
    if (_query.where)
    {
      let where_text = '';

      _query.where.forEach((con: any) =>
      {
        con.if.forEach((con_if: any) =>
        {
          const prefix = con_if.$.prefix || 'AND';

          if (this.evaluateCondition(con_if.$.test, _params))
          {
            where_text += `${prefix} ${con_if._.trim()} `;
          }
          else if (con_if.$.test.includes('NOT NULL'))
          {
            where_text += `${prefix} ${con_if._.trim()} IS NOT NULL `;
          }
          else if (con_if.$.test.includes('NULL'))
          {
            where_text += `${prefix} ${con_if._.trim()} IS NULL `;
          }
        });
      });

      if (where_text.length > 0)
      {
        where_text = where_text.trim().replace(/^(AND|OR|NOT)\s+/, '');
        sql += ` WHERE ${where_text}`;
      }
    }

    /**
     * <trim>
     * 
     */
    if (_query.trim)
    {
      const trimCondition = _query.trim[0];
      const prefix = trimCondition.$.prefix || '';
      const suffix = trimCondition.$.suffix || '';
      const overrides = (trimCondition.$.prefixOverrides || '').split('|');
      let trimContent = trimCondition._.trim();


      overrides.forEach((v: any) =>
      {
        const regex = new RegExp(`^${v.trim()}\\s+`);
        trimContent = trimContent.replace(regex, '');
      });

      sql += `${prefix} ${trimContent} ${suffix}`;
    }

    return sql.trim();
  }

  private evaluateCondition(_condition: string, _params: any): boolean
  {
    const l = _condition.split(' ')[0];
    const operator = _condition.split(' ')[1];
    const r = _condition.split(' ')[2];

    const lv = this.getNestedValue(_params, l);
    const rv = this.getValue(r, _params);

    switch (operator)
    {
      case '===':
        return lv === rv;

      case '!==':
        return lv !== rv;

      case '==':
        return lv == rv;

      case '!=':
        return lv != rv;

      case '>':
        return lv > rv;

      case '<':
        return lv < rv;

      case '>=':
        return lv >= rv;

      case '<=':
        return lv <= rv;

      default:
        return false;
    }
  }

  private getNestedValue(_obj: any, _path: string): any
  {
    return _path.split('.').reduce((o, key) => (o ? o[key] : undefined), _obj);
  }

  private getValue(_str: string, _params: any): any
  {
    if (_str === 'null')
    {
      return null;
    }

    if (_str === 'undefined')
    {
      return undefined;
    }

    if (!isNaN(Number(_str)))
    {
      return Number(_str);
    }

    return this.getNestedValue(_params, _str);
  }
}
