module.exports = legacy => {
  const ids = ['host-imports', 'host-service', 'host-internal-routes']
  const patches = ids.map(id => {
    const patch = legacy.find(candidate => candidate.id === id)
    return { ...patch, target: { ...patch.target, version: '>=0.1.5-0 <0.1.7-0' } }
  })
  patches[0].apply = ({ edit }) => {
    edit.prepend('import { ModernHostConnectionBinding as HostConnectionBinding } from "the-binding-of-dsh/host/modern";\n')
  }
  patches[2].select = 'MethodDeclaration[name.name="createSharedFetchHandler"] PropertyAssignment[name.name="fetch"] ArrowFunction > Block'
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
