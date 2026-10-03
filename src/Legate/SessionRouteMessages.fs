// SPDX-License-Identifier: Apache-2.0
namespace Legate

type internal SessionRouteProbe = SessionRouteProbe
type internal SessionRouteAccepted = SessionRouteAccepted

type internal SessionRouteRequest =
    {
        Address: string
        Scope: string
        Payload: obj
    }

type internal SessionRouteResponse =
    {
        Address: string
        Owner: string
        Payload: obj
    }

type internal SessionAddressedIngress = { EntityKey: string; Request: obj }
