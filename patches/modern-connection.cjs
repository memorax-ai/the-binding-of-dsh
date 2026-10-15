module.exports = legacy => {
  const ids = ['host-imports', 'host-service', 'host-internal-routes']
  const patches = ids.map(id => {
    const patch = legacy.find(candidate => candidate.id === id)
    return { ...patch, target: { ...patch.target, version: '>=0.1.5-0 <0.1.7-0 || 0.2.1-alpha.3' } }
  })
  patches[0].apply = ({ edit }) => {
    edit.prepend('import { ModernHostConnectionBinding as HostConnectionBinding } from "the-binding-of-dsh/host/modern";\n')
  }
  const attachService = patches[1].apply
  patches[1].select = 'VariableDeclaration[name.name="HostConnectionService"] ClassExpression > Constructor > Block'
  patches[1].apply = args => {
    const { node, sourceFile, edit, query, ts } = args
    // alpha.3 uses this private map; keep Binding's public peers API separate.
    const owner = node.parent.parent
    const peers = owner.members.find(member => member.name?.getText(sourceFile) === 'peers')
    if (peers) {
      edit.overwrite(peers.name.getStart(sourceFile), peers.name.getEnd(), '__bindingNativePeers')
      for (const access of query('PropertyAccessExpression', owner)) {
        if (access.expression.kind === ts.SyntaxKind.ThisKeyword && access.name.text === 'peers') {
          edit.overwrite(access.name.getStart(sourceFile), access.name.getEnd(), '__bindingNativePeers')
        }
      }
    }
    attachService(args)
  }
  patches[2].select = 'MethodDeclaration[name.name="createSharedFetchHandler"] PropertyAssignment[name.name="fetch"] > ArrowFunction > Block'
  patches[2].apply = ({ node, sourceFile, edit }) => {
    edit.appendLeft(node.getStart(sourceFile) + 1, `
      const connectionResponse = typeof peer === "undefined" || peer.principal === void 0 || peer.principal.kind === "owner"
        ? this.bidirectional.fetch(request) : void 0;
      if (connectionResponse !== void 0) return connectionResponse;`)
  }
  patches.push({
    id: 'modern-host-transport',
    description: 'Mount authenticated Binding sockets beside the native Gateway stream transport.',
    target: { ...patches[0].target },
    select: 'FunctionDeclaration[name.name="apply"] VariableStatement:has(VariableDeclaration[name.name="fetchHandler"])',
    expect: 1,
    apply({ node, edit }) {
      edit.prepend('import { installModernHost } from "the-binding-of-dsh/host/modern";\n')
      edit.appendRight(node.getEnd(), '\ninstallModernHost(ctx, connection, fetchHandler);')
    },
  })
  return [{
    id: 'bidirectional-connection',
    description: 'Install Binding host RPC on modern authenticated Connection transports.',
    patches,
  }]
}
