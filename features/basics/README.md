# Basics

## Ruby Objects to/from Apache Cassandra Datatypes

<table class="table table-striped table-hover table-condensed">
  <thead>
    <tr>
      <th>Ruby</th>
      <th>Cassandra</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td rowspan="4"><code>String</code></td>
      <td><code>ascii</code></td>
    </tr>
    <tr>
      <td><code>blob</code></td>
    </tr>
    <tr>
      <td><code>text</code></td>
    </tr>
    <tr>
      <td><code>varchar</code></td>
    </tr>
    <tr>
      <td rowspan="4"><code>Numeric</code></td>
      <td><code>bigint</code></td>
    </tr>
    <tr>
      <td><code>counter</code></td>
    </tr>
    <tr>
      <td><code>int</code></td>
    </tr>
    <tr>
      <td><code>varint</code></td>
    </tr>
    <tr>
      <td><code>Boolean</code></td>
      <td><code>boolean</code></td>
    </tr>
    <tr>
      <td><code>BigDecimal</code></td>
      <td><code>decimal</code></td>
    </tr>
    <tr>
      <td rowspan="2"><code>Float</code></td>
      <td><code>double</code></td>
    </tr>
    <tr>
      <td><code>float</code></td>
    </tr>
    <tr>
      <td><code>IPAddr</code></td>
      <td><code>inet</code></td>
    </tr>
    <tr>
      <td><code>Time</code></td>
      <td><code>timestamp</code></td>
    </tr>
    <tr>
      <td><code><a href="http://datastax.github.io/ruby-driver/api/uuid/">Cassandra::Uuid</a></code></td>
      <td><code>uuid</code></td>
    </tr>
    <tr>
      <td><code><a href="http://datastax.github.io/ruby-driver/api/time_uuid/">Cassandra::TimeUuid</a></code></td>
      <td><code>timeuuid</code></td>
    </tr>
    <tr>
      <td rowspan="2"><code>Array</code></td>
      <td><code>list</code></td>
    </tr>
    <tr>
      <td><code>tuple</code></td>
    </tr>
    <tr>
      <td><code>Set</code></td>
      <td><code>set</code></td>
    </tr>
    <tr>
      <td><code>Hash</code></td>
      <td><code>map</code></td>
    </tr>
    <tr>
      <td><code><a href="http://datastax.github.io/ruby-driver/api/user_value/">Cassandra::UserValue</a></code></td>
      <td><code>UDT</code> (user-defined type)</td>
    </tr>
  </tbody>
</table>
